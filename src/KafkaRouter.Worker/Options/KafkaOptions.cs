namespace KafkaRouter.Worker.Options;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; init; } = string.Empty;

    public string InputTopic { get; init; } = string.Empty;

    public string DeadLetterTopic { get; init; } = string.Empty;

    public string ConsumerGroupId { get; init; } = string.Empty;

    public string AutoOffsetReset { get; init; } = "Earliest";
}