using Confluent.Kafka;
using KafkaRouter.Worker.DeadLetter;
using KafkaRouter.Worker.Kafka;
using KafkaRouter.Worker.Models;
using KafkaRouter.Worker.MongoDb.Documents;
using KafkaRouter.Worker.MongoDb.Repositories;
using KafkaRouter.Worker.Options;
using KafkaRouter.Worker.Parsing;
using KafkaRouter.Worker.Routing;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace KafkaRouter.Worker;

public sealed class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IKafkaMessageConsumer _kafkaMessageConsumer;
    private readonly IKafkaMessageProducer _kafkaMessageProducer;
    private readonly IEventEnvelopeParser _eventEnvelopeParser;
    private readonly IEventRoutingService _eventRoutingService;
    private readonly IDeadLetterMessageFactory _deadLetterMessageFactory;
    private readonly IProcessedMessageRepository _processedMessageRepository;
    private readonly WorkerOptions _workerOptions;
    private readonly KafkaOptions _kafkaOptions;

    private int _consecutiveTechnicalFailures;

    public Worker(
        ILogger<Worker> logger,
        IKafkaMessageConsumer kafkaMessageConsumer,
        IKafkaMessageProducer kafkaMessageProducer,
        IEventEnvelopeParser eventEnvelopeParser,
        IEventRoutingService eventRoutingService,
        IDeadLetterMessageFactory deadLetterMessageFactory,
        IProcessedMessageRepository processedMessageRepository,
        IOptions<WorkerOptions> workerOptions,
        IOptions<KafkaOptions> kafkaOptions)
    {
        _logger = logger;
        _kafkaMessageConsumer = kafkaMessageConsumer;
        _kafkaMessageProducer = kafkaMessageProducer;
        _eventEnvelopeParser = eventEnvelopeParser;
        _eventRoutingService = eventRoutingService;
        _deadLetterMessageFactory = deadLetterMessageFactory;
        _processedMessageRepository = processedMessageRepository;
        _workerOptions = workerOptions.Value;
        _kafkaOptions = kafkaOptions.Value;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
    "Kafka Router Worker avviato. InstanceName: {InstanceName}.",
    _workerOptions.InstanceName);

        _kafkaMessageConsumer.Subscribe();

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _kafkaMessageConsumer.Consume(stoppingToken);

                    await ProcessMessageWithRetryAsync(
                        consumeResult,
                        stoppingToken);

                    ResetConsecutiveTechnicalFailures();
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (ConsumeException exception)
                {
                    await HandleTechnicalFailureAsync(
                        exception,
                        errorCategory: "KAFKA_CONSUME_ERROR",
                        message: "Errore durante la lettura da Kafka. Nessun offset verrà committato.",
                        stoppingToken);
                }
                catch (KafkaException exception)
                {
                    await HandleTechnicalFailureAsync(
                        exception,
                        errorCategory: "KAFKA_ERROR",
                        message: "Errore Kafka generico fuori dal processamento del singolo messaggio.",
                        stoppingToken);
                }
                catch (Exception exception)
                {
                    await HandleTechnicalFailureAsync(
                        exception,
                        errorCategory: "UNEXPECTED_WORKER_ERROR",
                        message: "Errore imprevisto nel loop principale del Worker.",
                        stoppingToken);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("Richiesta di arresto ricevuta.");
        }
        finally
        {
            _logger.LogInformation(
    "Kafka Router Worker arrestato correttamente. InstanceName: {InstanceName}.",
    _workerOptions.InstanceName);
        }
    }

    private async Task ProcessMessageWithRetryAsync(
        ConsumeResult<string, string> consumeResult,
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            for (var attempt = 1; attempt <= _workerOptions.TechnicalRetryMaxAttempts; attempt++)
            {
                try
                {
                    _logger.LogInformation(
                        "Avvio processamento messaggio. Topic: {Topic}. Partition: {Partition}. Offset: {Offset}. Attempt: {Attempt}/{MaxAttempts}.",
                        consumeResult.Topic,
                        consumeResult.Partition.Value,
                        consumeResult.Offset.Value,
                        attempt,
                        _workerOptions.TechnicalRetryMaxAttempts);

                    await ProcessMessageAsync(
                        consumeResult,
                        cancellationToken);

                    return;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception exception) when (IsTechnicalException(exception))
                {
                    _consecutiveTechnicalFailures++;

                    var retryDelay = CalculateRetryDelay(attempt);

                    _logger.LogError(
                        exception,
                        "Errore tecnico durante il processamento del messaggio. ErrorType: {ErrorType}. Topic: {Topic}. Partition: {Partition}. Offset: {Offset}. Attempt: {Attempt}/{MaxAttempts}. ConsecutiveTechnicalFailures: {ConsecutiveTechnicalFailures}. RetryDelaySeconds: {RetryDelaySeconds}. Il messaggio NON verrà committato.",
                        exception.GetType().Name,
                        consumeResult.Topic,
                        consumeResult.Partition.Value,
                        consumeResult.Offset.Value,
                        attempt,
                        _workerOptions.TechnicalRetryMaxAttempts,
                        _consecutiveTechnicalFailures,
                        retryDelay.TotalSeconds);

                    if (_consecutiveTechnicalFailures >= _workerOptions.ConsecutiveFailuresWarningThreshold)
                    {
                        _logger.LogCritical(
                            "Soglia di errori tecnici consecutivi raggiunta. ConsecutiveTechnicalFailures: {ConsecutiveTechnicalFailures}. Threshold: {Threshold}.",
                            _consecutiveTechnicalFailures,
                            _workerOptions.ConsecutiveFailuresWarningThreshold);
                    }

                    if (attempt < _workerOptions.TechnicalRetryMaxAttempts)
                    {
                        await Task.Delay(
                            retryDelay,
                            cancellationToken);
                    }
                }
            }

            _logger.LogCritical(
                "Tentativi tecnici esauriti per il messaggio corrente. Topic: {Topic}. Partition: {Partition}. Offset: {Offset}. MaxAttempts: {MaxAttempts}. Il messaggio resta NON committato. Attendo {DelayInSeconds} secondi e poi riprovo lo stesso messaggio.",
                consumeResult.Topic,
                consumeResult.Partition.Value,
                consumeResult.Offset.Value,
                _workerOptions.TechnicalRetryMaxAttempts,
                _workerOptions.ErrorDelayInSeconds);

            await DelayAfterErrorAsync(cancellationToken);
        }
    }

    private async Task ProcessMessageAsync(
        ConsumeResult<string, string> consumeResult,
        CancellationToken cancellationToken)
    {
        LogConsumedMessage(consumeResult);

        var parseResult = _eventEnvelopeParser.Parse(consumeResult.Message.Value);

        if (!parseResult.IsSuccess)
        {
            await ProduceToDeadLetterTopicAsync(
                consumeResult,
                parseResult.ErrorCode ?? "PARSE_ERROR",
                parseResult.ErrorMessage ?? "Errore non specificato durante il parsing del messaggio.",
                eventEnvelope: null,
                cancellationToken);

            _kafkaMessageConsumer.Commit(consumeResult);

            LogApplicationFailureHandled(
                consumeResult,
                parseResult.ErrorCode ?? "PARSE_ERROR",
                parseResult.ErrorMessage ?? "Errore non specificato durante il parsing del messaggio.");

            return;
        }

        var eventEnvelope = parseResult.EventEnvelope!;

        var alreadyProcessed = await _processedMessageRepository.ExistsByEventIdAsync(
            eventEnvelope.EventId!,
            cancellationToken);

        if (alreadyProcessed)
        {
            _logger.LogWarning(
                "Messaggio duplicato rilevato. EventId: {EventId}. EventType: {EventType}. Topic: {Topic}. Partition: {Partition}. Offset: {Offset}. Il messaggio non verrà riprodotto sui topic di destinazione.",
                eventEnvelope.EventId,
                eventEnvelope.EventType,
                consumeResult.Topic,
                consumeResult.Partition.Value,
                consumeResult.Offset.Value);

            _kafkaMessageConsumer.Commit(consumeResult);

            return;
        }

        var routingDecision = await _eventRoutingService.GetRoutingDecisionAsync(
            eventEnvelope,
            cancellationToken);

        if (!routingDecision.IsRoutable)
        {
            await ProduceToDeadLetterTopicAsync(
                consumeResult,
                routingDecision.ErrorCode ?? "ROUTING_ERROR",
                routingDecision.ErrorMessage ?? "Errore non specificato durante il routing del messaggio.",
                eventEnvelope,
                cancellationToken);

            _kafkaMessageConsumer.Commit(consumeResult);

            LogApplicationFailureHandled(
                consumeResult,
                routingDecision.ErrorCode ?? "ROUTING_ERROR",
                routingDecision.ErrorMessage ?? "Errore non specificato durante il routing del messaggio.");

            return;
        }

        await ProduceToDestinationTopicsAsync(
            consumeResult,
            eventEnvelope,
            routingDecision,
            cancellationToken);

        var processedMessage = CreateProcessedMessageDocument(
            consumeResult,
            eventEnvelope,
            routingDecision);

        var inserted = await _processedMessageRepository.TryInsertAsync(
            processedMessage,
            cancellationToken);

        if (!inserted)
        {
            _logger.LogWarning(
                "Il messaggio risulta già registrato come processato dopo la produzione. EventId: {EventId}. Possibile duplicato concorrente.",
                eventEnvelope.EventId);
        }

        _kafkaMessageConsumer.Commit(consumeResult);
    }

    private async Task ProduceToDestinationTopicsAsync(
        ConsumeResult<string, string> consumeResult,
        EventEnvelope eventEnvelope,
        RoutingDecision routingDecision,
        CancellationToken cancellationToken)
    {
        var effectiveKey = GetEffectiveMessageKey(
            consumeResult,
            eventEnvelope);

        foreach (var destinationTopic in routingDecision.DestinationTopics)
        {
            _logger.LogInformation(
                "Produzione evento verso topic destinazione. EventId: {EventId}. EventType: {EventType}. DestinationTopic: {DestinationTopic}.",
                eventEnvelope.EventId,
                eventEnvelope.EventType,
                destinationTopic);

            await _kafkaMessageProducer.ProduceAsync(
                destinationTopic,
                effectiveKey,
                consumeResult.Message.Value,
                cancellationToken);
        }

        _logger.LogInformation(
            "Evento instradato correttamente. EventId: {EventId}. EventType: {EventType}. DestinationTopics: {DestinationTopics}.",
            eventEnvelope.EventId,
            eventEnvelope.EventType,
            string.Join(", ", routingDecision.DestinationTopics));
    }

    private async Task ProduceToDeadLetterTopicAsync(
        ConsumeResult<string, string> consumeResult,
        string errorCode,
        string errorMessage,
        EventEnvelope? eventEnvelope,
        CancellationToken cancellationToken)
    {
        var deadLetterPayload = _deadLetterMessageFactory.CreateDeadLetterPayload(
            consumeResult,
            errorCode,
            errorMessage,
            eventEnvelope);

        var deadLetterKey = eventEnvelope?.EventId
            ?? consumeResult.Message.Key
            ?? $"{consumeResult.Topic}-{consumeResult.Partition.Value}-{consumeResult.Offset.Value}";

        _logger.LogWarning(
            "Messaggio inviato in DLQ. ErrorCode: {ErrorCode}. ErrorMessage: {ErrorMessage}. DeadLetterTopic: {DeadLetterTopic}. OriginalTopic: {OriginalTopic}. OriginalPartition: {OriginalPartition}. OriginalOffset: {OriginalOffset}.",
            errorCode,
            errorMessage,
            _kafkaOptions.DeadLetterTopic,
            consumeResult.Topic,
            consumeResult.Partition.Value,
            consumeResult.Offset.Value);

        await _kafkaMessageProducer.ProduceAsync(
            _kafkaOptions.DeadLetterTopic,
            deadLetterKey,
            deadLetterPayload,
            cancellationToken);
    }

    private static ProcessedMessageDocument CreateProcessedMessageDocument(
        ConsumeResult<string, string> consumeResult,
        EventEnvelope eventEnvelope,
        RoutingDecision routingDecision)
    {
        return new ProcessedMessageDocument
        {
            EventId = eventEnvelope.EventId!,
            EventType = eventEnvelope.EventType!,
            SourceTopic = consumeResult.Topic,
            SourcePartition = consumeResult.Partition.Value,
            SourceOffset = consumeResult.Offset.Value,
            DestinationTopics = routingDecision.DestinationTopics.ToArray(),
            ProcessedAt = DateTimeOffset.UtcNow
        };
    }

    private async Task HandleTechnicalFailureAsync(
        Exception exception,
        string errorCategory,
        string message,
        CancellationToken stoppingToken)
    {
        _consecutiveTechnicalFailures++;

        _logger.LogError(
            exception,
            "{Message} ErrorCategory: {ErrorCategory}. ConsecutiveTechnicalFailures: {ConsecutiveTechnicalFailures}. Attendo {DelayInSeconds} secondi prima di continuare.",
            message,
            errorCategory,
            _consecutiveTechnicalFailures,
            _workerOptions.ErrorDelayInSeconds);

        if (_consecutiveTechnicalFailures >= _workerOptions.ConsecutiveFailuresWarningThreshold)
        {
            _logger.LogCritical(
                "Soglia di errori tecnici consecutivi raggiunta. ConsecutiveTechnicalFailures: {ConsecutiveTechnicalFailures}. Threshold: {Threshold}.",
                _consecutiveTechnicalFailures,
                _workerOptions.ConsecutiveFailuresWarningThreshold);
        }

        await DelayAfterErrorAsync(stoppingToken);
    }

    private TimeSpan CalculateRetryDelay(int attempt)
    {
        var exponentialDelay = _workerOptions.TechnicalRetryInitialDelayInSeconds
            * Math.Pow(2, attempt - 1);

        var cappedDelay = Math.Min(
            exponentialDelay,
            _workerOptions.TechnicalRetryMaxDelayInSeconds);

        return TimeSpan.FromSeconds(cappedDelay);
    }

    private static bool IsTechnicalException(Exception exception)
    {
        return exception switch
        {
            ProduceException<string, string> => true,
            KafkaException => true,
            MongoException => true,
            TimeoutException => true,
            IOException => true,
            _ => true
        };
    }

    private void ResetConsecutiveTechnicalFailures()
    {
        if (_consecutiveTechnicalFailures == 0)
        {
            return;
        }

        _logger.LogInformation(
            "Processamento tornato a buon fine dopo {ConsecutiveTechnicalFailures} errori tecnici consecutivi.",
            _consecutiveTechnicalFailures);

        _consecutiveTechnicalFailures = 0;
    }

    private void LogApplicationFailureHandled(
        ConsumeResult<string, string> consumeResult,
        string errorCode,
        string errorMessage)
    {
        _logger.LogWarning(
            "Errore applicativo gestito con DLQ e commit. ErrorCode: {ErrorCode}. ErrorMessage: {ErrorMessage}. Topic: {Topic}. Partition: {Partition}. Offset: {Offset}.",
            errorCode,
            errorMessage,
            consumeResult.Topic,
            consumeResult.Partition.Value,
            consumeResult.Offset.Value);
    }

    private static string? GetEffectiveMessageKey(
        ConsumeResult<string, string> consumeResult,
        EventEnvelope eventEnvelope)
    {
        if (!string.IsNullOrWhiteSpace(consumeResult.Message.Key))
        {
            return consumeResult.Message.Key;
        }

        if (!string.IsNullOrWhiteSpace(eventEnvelope.EventId))
        {
            return eventEnvelope.EventId;
        }

        return null;
    }

    private async Task DelayAfterErrorAsync(CancellationToken stoppingToken)
    {
        await Task.Delay(
            TimeSpan.FromSeconds(_workerOptions.ErrorDelayInSeconds),
            stoppingToken);
    }

    private void LogConsumedMessage(ConsumeResult<string, string> consumeResult)
    {
        _logger.LogInformation(
            """
            Messaggio Kafka ricevuto.
            Topic: {Topic}
            Partition: {Partition}
            Offset: {Offset}
            Key: {Key}
            Value: {Value}
            """,
            consumeResult.Topic,
            consumeResult.Partition.Value,
            consumeResult.Offset.Value,
            consumeResult.Message.Key,
            consumeResult.Message.Value);
    }
}