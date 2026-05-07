namespace KafkaRouter.Worker.Metrics;

public interface IWorkerMetrics
{
    void IncrementProcessedMessages(
        string eventId,
        string eventType);

    void IncrementDeadLetterMessages(
        string? eventId,
        string? eventType,
        string errorCode);

    void IncrementDuplicateMessages(
        string eventId,
        string eventType);

    void IncrementTechnicalFailures(
        string errorCategory);

    WorkerMetricsSnapshot GetSnapshot();
}