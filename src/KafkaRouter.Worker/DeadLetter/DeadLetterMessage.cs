namespace KafkaRouter.Worker.DeadLetter;

public sealed class DeadLetterMessage
{
    public string OriginalTopic { get; init; } = string.Empty;

    public int OriginalPartition { get; init; }

    public long OriginalOffset { get; init; }

    public string? OriginalKey { get; init; }

    public string ErrorCode { get; init; } = string.Empty;

    public string ErrorMessage { get; init; } = string.Empty;

    public DateTimeOffset FailedAt { get; init; }

    public string? EventId { get; init; }

    public string? EventType { get; init; }

    public string? CorrelationId { get; init; }

    public string? OriginalPayload { get; init; }
}