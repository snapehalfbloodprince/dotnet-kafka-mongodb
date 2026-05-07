using Confluent.Kafka;
using KafkaRouter.Worker.Kafka;
using KafkaRouter.Worker.Metrics;
using KafkaRouter.Worker.Options;
using KafkaRouter.Worker.Processing;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace KafkaRouter.Worker;

public sealed class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IKafkaMessageConsumer _kafkaMessageConsumer;
    private readonly IMessageProcessingService _messageProcessingService;
    private readonly IWorkerMetrics _workerMetrics;
    private readonly WorkerOptions _workerOptions;

    private int _consecutiveTechnicalFailures;

    public Worker(
        ILogger<Worker> logger,
        IKafkaMessageConsumer kafkaMessageConsumer,
        IMessageProcessingService messageProcessingService,
        IWorkerMetrics workerMetrics,
        IOptions<WorkerOptions> workerOptions)
    {
        _logger = logger;
        _kafkaMessageConsumer = kafkaMessageConsumer;
        _messageProcessingService = messageProcessingService;
        _workerMetrics = workerMetrics;
        _workerOptions = workerOptions.Value;
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

                    var processingResult = await _messageProcessingService.ProcessAsync(
                        consumeResult,
                        cancellationToken);

                    _logger.LogInformation(
                        "Processamento messaggio completato. Outcome: {Outcome}. EventId: {EventId}. EventType: {EventType}. Topic: {Topic}. Partition: {Partition}. Offset: {Offset}.",
                        processingResult.Outcome,
                        processingResult.EventId,
                        processingResult.EventType,
                        consumeResult.Topic,
                        consumeResult.Partition.Value,
                        consumeResult.Offset.Value);

                    return;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception exception) when (IsTechnicalException(exception))
                {
                    _consecutiveTechnicalFailures++;

                    _workerMetrics.IncrementTechnicalFailures("MESSAGE_PROCESSING_TECHNICAL_ERROR");

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

    private async Task HandleTechnicalFailureAsync(
        Exception exception,
        string errorCategory,
        string message,
        CancellationToken stoppingToken)
    {
        _consecutiveTechnicalFailures++;

        _workerMetrics.IncrementTechnicalFailures(errorCategory);

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

    private async Task DelayAfterErrorAsync(CancellationToken stoppingToken)
    {
        await Task.Delay(
            TimeSpan.FromSeconds(_workerOptions.ErrorDelayInSeconds),
            stoppingToken);
    }
}