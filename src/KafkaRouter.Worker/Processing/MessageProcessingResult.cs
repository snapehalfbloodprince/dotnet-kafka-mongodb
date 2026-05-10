namespace KafkaRouter.Worker.Processing;

public sealed class MessageProcessingResult
{
    private MessageProcessingResult(
        MessageProcessingOutcome outcome,
        string? eventId,
        string? eventType,
        string? correlationId,
        long processingDurationMs,
        string? errorCode,
        string? errorMessage)
    {
        Outcome = outcome;
        EventId = eventId;
        EventType = eventType;
        CorrelationId = correlationId;
        ProcessingDurationMs = processingDurationMs;
        ErrorCode = errorCode;
        ErrorMessage = errorMessage;
    }

    public MessageProcessingOutcome Outcome { get; }

    public string? EventId { get; }

    public string? EventType { get; }

    public string? CorrelationId { get; }

    public long ProcessingDurationMs { get; }

    public string? ErrorCode { get; }

    public string? ErrorMessage { get; }

    public bool IsSuccessful => Outcome == MessageProcessingOutcome.ProcessedSuccessfully;

    public bool IsDeadLetter => Outcome == MessageProcessingOutcome.SentToDeadLetter;

    public bool IsDuplicate => Outcome == MessageProcessingOutcome.SkippedAsDuplicate;

    public static MessageProcessingResult ProcessedSuccessfully(
        string eventId,
        string eventType,
        string? correlationId,
        long processingDurationMs)
    {
        return new MessageProcessingResult(
            MessageProcessingOutcome.ProcessedSuccessfully,
            eventId,
            eventType,
            correlationId,
            processingDurationMs,
            errorCode: null,
            errorMessage: null);
    }

    public static MessageProcessingResult SentToDeadLetter(
        string? eventId,
        string? eventType,
        string? correlationId,
        long processingDurationMs,
        string errorCode,
        string errorMessage)
    {
        return new MessageProcessingResult(
            MessageProcessingOutcome.SentToDeadLetter,
            eventId,
            eventType,
            correlationId,
            processingDurationMs,
            errorCode,
            errorMessage);
    }

    public static MessageProcessingResult SkippedAsDuplicate(
        string eventId,
        string eventType,
        string? correlationId,
        long processingDurationMs)
    {
        return new MessageProcessingResult(
            MessageProcessingOutcome.SkippedAsDuplicate,
            eventId,
            eventType,
            correlationId,
            processingDurationMs,
            errorCode: null,
            errorMessage: null);
    }
}