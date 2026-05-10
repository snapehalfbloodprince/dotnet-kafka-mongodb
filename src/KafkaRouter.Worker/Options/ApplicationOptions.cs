namespace KafkaRouter.Worker.Options;

public sealed class ApplicationOptions
{
    public const string SectionName = "Application";

    public string Name { get; init; } = "KafkaRouter.Worker";

    public string Environment { get; init; } = "Local";
}