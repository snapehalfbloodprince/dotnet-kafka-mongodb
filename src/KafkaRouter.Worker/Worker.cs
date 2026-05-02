using Confluent.Kafka;
using KafkaRouter.Worker.DeadLetter;
using KafkaRouter.Worker.Kafka;
using KafkaRouter.Worker.Models;
using KafkaRouter.Worker.Options;
using KafkaRouter.Worker.Parsing;
using KafkaRouter.Worker.Routing;
using Microsoft.Extensions.Options;

namespace KafkaRouter.Worker;

public sealed class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IKafkaMessageConsumer _kafkaMessageConsumer;
    private readonly IKafkaMessageProducer _kafkaMessageProducer;
    private readonly IEventEnvelopeParser _eventEnvelopeParser;
    private readonly IEventRoutingService _eventRoutingService;
    private readonly IDeadLetterMessageFactory _deadLetterMessageFactory;
    private readonly WorkerOptions _workerOptions;
    private readonly KafkaOptions _kafkaOptions;

    public Worker(
        ILogger<Worker> logger,
        IKafkaMessageConsumer kafkaMessageConsumer,
        IKafkaMessageProducer kafkaMessageProducer,
        IEventEnvelopeParser eventEnvelopeParser,
        IEventRoutingService eventRoutingService,
        IDeadLetterMessageFactory deadLetterMessageFactory,
        IOptions<WorkerOptions> workerOptions,
        IOptions<KafkaOptions> kafkaOptions)
    {
        _logger = logger;
        _kafkaMessageConsumer = kafkaMessageConsumer;
        _kafkaMessageProducer = kafkaMessageProducer;
        _eventEnvelopeParser = eventEnvelopeParser;
        _eventRoutingService = eventRoutingService;
        _deadLetterMessageFactory = deadLetterMessageFactory;
        _workerOptions = workerOptions.Value;
        _kafkaOptions = kafkaOptions.Value;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Kafka Router Worker avviato.");

        _kafkaMessageConsumer.Subscribe();

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _kafkaMessageConsumer.Consume(stoppingToken);

                    await ProcessMessageAsync(
                        consumeResult,
                        stoppingToken);
                }
                catch (ConsumeException exception)
                {
                    _logger.LogError(
                        exception,
                        "Errore durante la lettura da Kafka. Attendo {DelayInSeconds} secondi prima di riprovare.",
                        _workerOptions.ErrorDelayInSeconds);

                    await DelayAfterErrorAsync(stoppingToken);
                }
                catch (ProduceException<string, string> exception)
                {
                    _logger.LogError(
                        exception,
                        "Errore durante la produzione su Kafka. Il messaggio non verrà committato. Attendo {DelayInSeconds} secondi prima di riprovare.",
                        _workerOptions.ErrorDelayInSeconds);

                    await DelayAfterErrorAsync(stoppingToken);
                }
                catch (KafkaException exception)
                {
                    _logger.LogError(
                        exception,
                        "Errore Kafka generico. Attendo {DelayInSeconds} secondi prima di riprovare.",
                        _workerOptions.ErrorDelayInSeconds);

                    await DelayAfterErrorAsync(stoppingToken);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("Richiesta di arresto ricevuta.");
        }
        finally
        {
            _logger.LogInformation("Kafka Router Worker arrestato correttamente.");
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

            return;
        }

        var eventEnvelope = parseResult.EventEnvelope!;

        var routingDecision = _eventRoutingService.GetRoutingDecision(eventEnvelope);

        if (!routingDecision.IsRoutable)
        {
            await ProduceToDeadLetterTopicAsync(
                consumeResult,
                routingDecision.ErrorCode ?? "ROUTING_ERROR",
                routingDecision.ErrorMessage ?? "Errore non specificato durante il routing del messaggio.",
                eventEnvelope,
                cancellationToken);

            _kafkaMessageConsumer.Commit(consumeResult);

            return;
        }

        await ProduceToDestinationTopicsAsync(
            consumeResult,
            eventEnvelope,
            routingDecision,
            cancellationToken);

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