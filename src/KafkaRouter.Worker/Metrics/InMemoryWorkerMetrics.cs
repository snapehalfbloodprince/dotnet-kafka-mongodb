using KafkaRouter.Worker.Options;
using Microsoft.Extensions.Options;

namespace KafkaRouter.Worker.Metrics;

public sealed class InMemoryWorkerMetrics : IWorkerMetrics
{
    private readonly object _syncRoot = new();
    private readonly WorkerOptions _workerOptions;
    private readonly DateTimeOffset _startedAt = DateTimeOffset.UtcNow;

    private long _processedMessages;
    private long _deadLetterMessages;
    private long _duplicateMessages;
    private long _technicalFailures;

    private string? _lastProcessedEventId;
    private string? _lastProcessedEventType;
    private DateTimeOffset? _lastProcessedAt;
    private long? _lastProcessedDurationMs;

    private string? _lastDeadLetterEventId;
    private string? _lastDeadLetterEventType;
    private string? _lastDeadLetterErrorCode;
    private DateTimeOffset? _lastDeadLetterAt;
    private long? _lastDeadLetterDurationMs;

    private string? _lastDuplicateEventId;
    private string? _lastDuplicateEventType;
    private DateTimeOffset? _lastDuplicateAt;
    private long? _lastDuplicateDurationMs;

    private string? _lastTechnicalFailureCategory;
    private DateTimeOffset? _lastTechnicalFailureAt;

    private long _totalProcessingDurationMs;
    private long? _maxProcessingDurationMs;

    public InMemoryWorkerMetrics(IOptions<WorkerOptions> workerOptions)
    {
        _workerOptions = workerOptions.Value;
    }

    public void IncrementProcessedMessages(
        string eventId,
        string eventType,
        long processingDurationMs)
    {
        lock (_syncRoot)
        {
            var normalizedDuration = NormalizeDuration(processingDurationMs);

            _processedMessages++;

            _lastProcessedEventId = eventId;
            _lastProcessedEventType = eventType;
            _lastProcessedAt = DateTimeOffset.UtcNow;
            _lastProcessedDurationMs = normalizedDuration;

            TrackProcessingDuration(normalizedDuration);
        }
    }

    public void IncrementDeadLetterMessages(
        string? eventId,
        string? eventType,
        string errorCode,
        long processingDurationMs)
    {
        lock (_syncRoot)
        {
            var normalizedDuration = NormalizeDuration(processingDurationMs);

            _deadLetterMessages++;

            _lastDeadLetterEventId = eventId;
            _lastDeadLetterEventType = eventType;
            _lastDeadLetterErrorCode = errorCode;
            _lastDeadLetterAt = DateTimeOffset.UtcNow;
            _lastDeadLetterDurationMs = normalizedDuration;

            TrackProcessingDuration(normalizedDuration);
        }
    }

    public void IncrementDuplicateMessages(
        string eventId,
        string eventType,
        long processingDurationMs)
    {
        lock (_syncRoot)
        {
            var normalizedDuration = NormalizeDuration(processingDurationMs);

            _duplicateMessages++;

            _lastDuplicateEventId = eventId;
            _lastDuplicateEventType = eventType;
            _lastDuplicateAt = DateTimeOffset.UtcNow;
            _lastDuplicateDurationMs = normalizedDuration;

            TrackProcessingDuration(normalizedDuration);
        }
    }

    public void IncrementTechnicalFailures(string errorCategory)
    {
        lock (_syncRoot)
        {
            _technicalFailures++;

            _lastTechnicalFailureCategory = errorCategory;
            _lastTechnicalFailureAt = DateTimeOffset.UtcNow;
        }
    }

    public WorkerMetricsSnapshot GetSnapshot()
    {
        lock (_syncRoot)
        {
            var completedMessages =
                _processedMessages
                + _deadLetterMessages
                + _duplicateMessages;

            var averageProcessingDurationMs = completedMessages > 0
                ? _totalProcessingDurationMs / completedMessages
                : (long?)null;

            return new WorkerMetricsSnapshot
            {
                InstanceName = _workerOptions.InstanceName,
                StartedAt = _startedAt,
                CheckedAt = DateTimeOffset.UtcNow,

                ProcessedMessages = _processedMessages,
                DeadLetterMessages = _deadLetterMessages,
                DuplicateMessages = _duplicateMessages,
                TechnicalFailures = _technicalFailures,

                LastProcessedEventId = _lastProcessedEventId,
                LastProcessedEventType = _lastProcessedEventType,
                LastProcessedAt = _lastProcessedAt,
                LastProcessedDurationMs = _lastProcessedDurationMs,

                LastDeadLetterEventId = _lastDeadLetterEventId,
                LastDeadLetterEventType = _lastDeadLetterEventType,
                LastDeadLetterErrorCode = _lastDeadLetterErrorCode,
                LastDeadLetterAt = _lastDeadLetterAt,
                LastDeadLetterDurationMs = _lastDeadLetterDurationMs,

                LastDuplicateEventId = _lastDuplicateEventId,
                LastDuplicateEventType = _lastDuplicateEventType,
                LastDuplicateAt = _lastDuplicateAt,
                LastDuplicateDurationMs = _lastDuplicateDurationMs,

                LastTechnicalFailureCategory = _lastTechnicalFailureCategory,
                LastTechnicalFailureAt = _lastTechnicalFailureAt,

                TotalProcessingDurationMs = _totalProcessingDurationMs,
                AverageProcessingDurationMs = averageProcessingDurationMs,
                MaxProcessingDurationMs = _maxProcessingDurationMs
            };
        }
    }

    private static long NormalizeDuration(long processingDurationMs)
    {
        return Math.Max(0, processingDurationMs);
    }

    private void TrackProcessingDuration(long processingDurationMs)
    {
        _totalProcessingDurationMs += processingDurationMs;

        if (_maxProcessingDurationMs is null
            || processingDurationMs > _maxProcessingDurationMs)
        {
            _maxProcessingDurationMs = processingDurationMs;
        }
    }
}