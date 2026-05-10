namespace KafkaRouter.Worker.Metrics;

public sealed class WorkerMetricsSnapshot
{
    public string InstanceName { get; init; } = string.Empty;

    public DateTimeOffset StartedAt { get; init; }

    public DateTimeOffset CheckedAt { get; init; }

    public long ProcessedMessages { get; init; }

    public long DeadLetterMessages { get; init; }

    public long DuplicateMessages { get; init; }

    public long TechnicalFailures { get; init; }

    public string? LastProcessedEventId { get; init; }

    public string? LastProcessedEventType { get; init; }

    public DateTimeOffset? LastProcessedAt { get; init; }

    public long? LastProcessedDurationMs { get; init; }

    public string? LastDeadLetterEventId { get; init; }

    public string? LastDeadLetterEventType { get; init; }

    public string? LastDeadLetterErrorCode { get; init; }

    public DateTimeOffset? LastDeadLetterAt { get; init; }

    public long? LastDeadLetterDurationMs { get; init; }

    public string? LastDuplicateEventId { get; init; }

    public string? LastDuplicateEventType { get; init; }

    public DateTimeOffset? LastDuplicateAt { get; init; }

    public long? LastDuplicateDurationMs { get; init; }

    public string? LastTechnicalFailureCategory { get; init; }

    public DateTimeOffset? LastTechnicalFailureAt { get; init; }

    public long TotalProcessingDurationMs { get; init; }

    public long? AverageProcessingDurationMs { get; init; }

    public long? MaxProcessingDurationMs { get; init; }
}