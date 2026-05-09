using Confluent.Kafka;
using KafkaRouter.Worker.Metrics;
using KafkaRouter.Worker.Options;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using Polly;
using Polly.Retry;

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
            var retryPipeline = CreateRetryPipeline(consumeResult);

            try
            {
                var result = await retryPipeline.ExecuteAsync(
                    async token => await _messageProcessingService.ProcessAsync(
                        consumeResult,
                        token),
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
                _logger.LogCritical(
                    exception,
                    "Tentativi tecnici esauriti per il messaggio corrente. Topic: {Topic}. Partition: {Partition}. Offset: {Offset}. MaxAttempts: {MaxAttempts}. Il messaggio resta NON committato. Attendo {DelayInSeconds} secondi e poi riprovo lo stesso messaggio.",
                    consumeResult.Topic,
                    consumeResult.Partition.Value,
                    consumeResult.Offset.Value,
                    _workerOptions.TechnicalRetryMaxAttempts,
                    _workerOptions.ErrorDelayInSeconds);

                await DelayAfterErrorAsync(cancellationToken);
            }
        }

        throw new OperationCanceledException(cancellationToken);
    }

    private ResiliencePipeline<MessageProcessingResult> CreateRetryPipeline(
        ConsumeResult<string, string> consumeResult)
    {
        var retryCount = Math.Max(
            0,
            _workerOptions.TechnicalRetryMaxAttempts - 1);

        var retryOptions = new RetryStrategyOptions<MessageProcessingResult>
        {
            MaxRetryAttempts = retryCount,
            DelayGenerator = arguments =>
            {
                var delay = CalculateRetryDelay(arguments.AttemptNumber + 1);

                return new ValueTask<TimeSpan?>(delay);
            },
            ShouldHandle = arguments =>
            {
                if (arguments.Outcome.Exception is null)
                {
                    return new ValueTask<bool>(false);
                }

                return new ValueTask<bool>(
                    IsTechnicalException(arguments.Outcome.Exception));
            },
            OnRetry = arguments =>
            {
                var exception = arguments.Outcome.Exception;

                if (exception is not null)
                {
                    HandleRetryAttempt(
                        exception,
                        consumeResult,
                        arguments.AttemptNumber + 1,
                        arguments.RetryDelay);
                }

                return default;
            }
        };

        return new ResiliencePipelineBuilder<MessageProcessingResult>()
            .AddRetry(retryOptions)
            .Build();
    }

    private void HandleRetryAttempt(
        Exception exception,
        ConsumeResult<string, string> consumeResult,
        int retryAttempt,
        TimeSpan retryDelay)
    {
        _consecutiveTechnicalFailures++;

        _workerMetrics.IncrementTechnicalFailures("MESSAGE_PROCESSING_TECHNICAL_ERROR");

        _logger.LogError(
            exception,
            "Errore tecnico durante il processamento del messaggio. ErrorType: {ErrorType}. Topic: {Topic}. Partition: {Partition}. Offset: {Offset}. RetryAttempt: {RetryAttempt}/{MaxRetryAttempts}. ConsecutiveTechnicalFailures: {ConsecutiveTechnicalFailures}. RetryDelaySeconds: {RetryDelaySeconds}. Il messaggio NON verrà committato.",
            exception.GetType().Name,
            consumeResult.Topic,
            consumeResult.Partition.Value,
            consumeResult.Offset.Value,
            retryAttempt,
            _workerOptions.TechnicalRetryMaxAttempts - 1,
            _consecutiveTechnicalFailures,
            retryDelay.TotalSeconds);

        if (_consecutiveTechnicalFailures >= _workerOptions.ConsecutiveFailuresWarningThreshold)
        {
            _logger.LogCritical(
                "Soglia di errori tecnici consecutivi raggiunta. ConsecutiveTechnicalFailures: {ConsecutiveTechnicalFailures}. Threshold: {Threshold}.",
                _consecutiveTechnicalFailures,
                _workerOptions.ConsecutiveFailuresWarningThreshold);
        }
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