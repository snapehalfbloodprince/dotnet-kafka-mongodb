namespace KafkaRouter.Worker.Options;

public sealed class WorkerOptions
{
    public const string SectionName = "Worker";
    public int DelayInSeconds { get; init; } = 2;

    public int ErrorDelayInSeconds { get; init; } = 5;
}