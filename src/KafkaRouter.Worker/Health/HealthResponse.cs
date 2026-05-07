namespace KafkaRouter.Worker.Health;

public sealed class HealthResponse
{
    public string Status { get; init; } = string.Empty;

    public string InstanceName { get; init; } = string.Empty;

    public DateTimeOffset CheckedAt { get; init; }

    public Dictionary<string, string> Checks { get; init; } = new();
}