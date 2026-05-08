using Confluent.Kafka;
using KafkaRouter.Worker.Metrics;
using KafkaRouter.Worker.Options;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace KafkaRouter.Worker.Processing;

public sealed class MessageProcessingRetryService : IMessageProcessingRetryService
{
    private readonly ILogger<MessageProcessingRetryService> _logger;
    private readonly IMessageProcessingService _messageProcessingService;
    private readonly IWorkerMetrics _workerMetrics;
    private readonly WorkerOptions _workerOptions;

    private int _consecutiveTechnicalFailures;

    public MessageProcessingRetryService(
        ILogger<MessageProcessingRetryService> logger,
        IMessageProcessingService messageProcessingService,
        IWorkerMetrics workerMetrics,
        IOptions<WorkerOptions> workerOptions)
    {
        _logger = logger;
        _messageProcessingService = messageProcessingService;
        _workerMetrics = workerMetrics;
        _workerOptions = workerOptions.Value;
    }

    public async Task<MessageProcessingResult> ProcessWithRetryAsync(
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

                    var result = await _messageProcessingService.ProcessAsync(
                        consumeResult,
                        cancellationToken);

                    ResetConsecutiveTechnicalFailures();

                    return result;
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

        throw new OperationCanceledException(cancellationToken);
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