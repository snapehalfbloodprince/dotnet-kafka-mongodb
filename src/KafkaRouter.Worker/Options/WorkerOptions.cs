namespace KafkaRouter.Worker.Options;

public sealed class WorkerOptions
{
    public const string SectionName = "Worker";

    public string InstanceName { get; init; } = Environment.MachineName;

    public int ErrorDelayInSeconds { get; init; } = 5;

    public int ConsecutiveFailuresWarningThreshold { get; init; } = 5;

    public int TechnicalRetryMaxAttempts { get; init; } = 3;

    public int TechnicalRetryInitialDelayInSeconds { get; init; } = 1;

    public int TechnicalRetryMaxDelayInSeconds { get; init; } = 10;
}