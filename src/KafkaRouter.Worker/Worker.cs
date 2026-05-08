using Confluent.Kafka;
using KafkaRouter.Worker.Kafka;
using KafkaRouter.Worker.Options;
using KafkaRouter.Worker.Processing;
using Microsoft.Extensions.Options;

namespace KafkaRouter.Worker;

public sealed class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IKafkaMessageConsumer _kafkaMessageConsumer;
    private readonly IMessageProcessingRetryService _messageProcessingRetryService;
    private readonly WorkerOptions _workerOptions;

    private int _consecutiveTechnicalFailures;

    public Worker(
        ILogger<Worker> logger,
        IKafkaMessageConsumer kafkaMessageConsumer,
        IMessageProcessingRetryService messageProcessingRetryService,
        IOptions<WorkerOptions> workerOptions)
    {
        _logger = logger;
        _kafkaMessageConsumer = kafkaMessageConsumer;
        _messageProcessingRetryService = messageProcessingRetryService;
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

                    var processingResult = await _messageProcessingRetryService.ProcessWithRetryAsync(
                        consumeResult,
                        stoppingToken);

                    _logger.LogInformation(
                        "Processamento messaggio completato. Outcome: {Outcome}. EventId: {EventId}. EventType: {EventType}. CorrelationId: {CorrelationId}. Topic: {Topic}. Partition: {Partition}. Offset: {Offset}.",
                        processingResult.Outcome,
                        processingResult.EventId,
                        processingResult.EventType,
                        processingResult.CorrelationId,
                        consumeResult.Topic,
                        consumeResult.Partition.Value,
                        consumeResult.Offset.Value);

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

    private void ResetConsecutiveTechnicalFailures()
    {
        if (_consecutiveTechnicalFailures == 0)
        {
            return;
        }

        _logger.LogInformation(
            "Loop Worker tornato a buon fine dopo {ConsecutiveTechnicalFailures} errori tecnici consecutivi.",
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