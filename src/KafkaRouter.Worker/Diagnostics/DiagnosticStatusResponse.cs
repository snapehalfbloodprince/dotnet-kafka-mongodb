using KafkaRouter.Worker.Metrics;

namespace KafkaRouter.Worker.Diagnostics;

public sealed class DiagnosticStatusResponse
{
    public string ApplicationName { get; init; } = string.Empty;

    public string ApplicationEnvironment { get; init; } = string.Empty;

    public string InstanceName { get; init; } = string.Empty;

    public DateTimeOffset CheckedAt { get; init; }

    public string KafkaStatus { get; init; } = string.Empty;

    public string MongoDbStatus { get; init; } = string.Empty;

    public WorkerMetricsSnapshot Metrics { get; init; } = new();
}