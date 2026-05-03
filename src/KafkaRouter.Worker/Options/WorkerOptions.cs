namespace KafkaRouter.Worker.Options;

public sealed class WorkerOptions
{
    public const string SectionName = "Worker";

    public int ErrorDelayInSeconds { get; init; } = 5;

    public int ConsecutiveFailuresWarningThreshold { get; init; } = 5;
}