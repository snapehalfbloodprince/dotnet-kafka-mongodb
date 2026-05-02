namespace KafkaRouter.Worker.Options;

public sealed class MongoDbOptions
{
    public const string SectionName = "MongoDb";

    public string ConnectionString { get; init; } = string.Empty;

    public string DatabaseName { get; init; } = string.Empty;

    public string RoutingRulesCollectionName { get; init; } = string.Empty;
}