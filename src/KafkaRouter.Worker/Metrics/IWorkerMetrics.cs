namespace KafkaRouter.Worker.Metrics;

public interface IWorkerMetrics
{
    void IncrementProcessedMessages(
        string eventId,
        string eventType,
        long processingDurationMs);

    void IncrementDeadLetterMessages(
        string? eventId,
        string? eventType,
        string errorCode,
        long processingDurationMs);

    void IncrementDuplicateMessages(
        string eventId,
        string eventType,
        long processingDurationMs);

    void IncrementTechnicalFailures(string errorCategory);

    WorkerMetricsSnapshot GetSnapshot();
}