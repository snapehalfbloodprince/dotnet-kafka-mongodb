using KafkaRouter.Worker.Options;
using Microsoft.Extensions.Options;

namespace KafkaRouter.Worker.Metrics;

public sealed class InMemoryWorkerMetrics : IWorkerMetrics
{
    private readonly object _lock = new();
    private readonly string _instanceName;
    private readonly DateTimeOffset _startedAt = DateTimeOffset.UtcNow;

    private long _processedMessages;
    private long _deadLetterMessages;
    private long _duplicateMessages;
    private long _technicalFailures;

    private string? _lastProcessedEventId;
    private string? _lastProcessedEventType;
    private DateTimeOffset? _lastProcessedAt;

    private string? _lastDeadLetterEventId;
    private string? _lastDeadLetterEventType;
    private string? _lastDeadLetterErrorCode;
    private DateTimeOffset? _lastDeadLetterAt;

    private string? _lastDuplicateEventId;
    private string? _lastDuplicateEventType;
    private DateTimeOffset? _lastDuplicateAt;

    private string? _lastTechnicalFailureCategory;
    private DateTimeOffset? _lastTechnicalFailureAt;

    public InMemoryWorkerMetrics(IOptions<WorkerOptions> workerOptions)
    {
        _instanceName = workerOptions.Value.InstanceName;
    }

    public void IncrementProcessedMessages(
        string eventId,
        string eventType)
    {
        lock (_lock)
        {
            _processedMessages++;
            _lastProcessedEventId = eventId;
            _lastProcessedEventType = eventType;
            _lastProcessedAt = DateTimeOffset.UtcNow;
        }
    }

    public void IncrementDeadLetterMessages(
        string? eventId,
        string? eventType,
        string errorCode)
    {
        lock (_lock)
        {
            _deadLetterMessages++;
            _lastDeadLetterEventId = eventId;
            _lastDeadLetterEventType = eventType;
            _lastDeadLetterErrorCode = errorCode;
            _lastDeadLetterAt = DateTimeOffset.UtcNow;
        }
    }

    public void IncrementDuplicateMessages(
        string eventId,
        string eventType)
    {
        lock (_lock)
        {
            _duplicateMessages++;
            _lastDuplicateEventId = eventId;
            _lastDuplicateEventType = eventType;
            _lastDuplicateAt = DateTimeOffset.UtcNow;
        }
    }

    public void IncrementTechnicalFailures(
        string errorCategory)
    {
        lock (_lock)
        {
            _technicalFailures++;
            _lastTechnicalFailureCategory = errorCategory;
            _lastTechnicalFailureAt = DateTimeOffset.UtcNow;
        }
    }

    public WorkerMetricsSnapshot GetSnapshot()
    {
        lock (_lock)
        {
            return new WorkerMetricsSnapshot
            {
                InstanceName = _instanceName,
                StartedAt = _startedAt,
                CheckedAt = DateTimeOffset.UtcNow,

                ProcessedMessages = _processedMessages,
                DeadLetterMessages = _deadLetterMessages,
                DuplicateMessages = _duplicateMessages,
                TechnicalFailures = _technicalFailures,

                LastProcessedEventId = _lastProcessedEventId,
                LastProcessedEventType = _lastProcessedEventType,
                LastProcessedAt = _lastProcessedAt,

                LastDeadLetterEventId = _lastDeadLetterEventId,
                LastDeadLetterEventType = _lastDeadLetterEventType,
                LastDeadLetterErrorCode = _lastDeadLetterErrorCode,
                LastDeadLetterAt = _lastDeadLetterAt,

                LastDuplicateEventId = _lastDuplicateEventId,
                LastDuplicateEventType = _lastDuplicateEventType,
                LastDuplicateAt = _lastDuplicateAt,

                LastTechnicalFailureCategory = _lastTechnicalFailureCategory,
                LastTechnicalFailureAt = _lastTechnicalFailureAt
            };
        }
    }
}