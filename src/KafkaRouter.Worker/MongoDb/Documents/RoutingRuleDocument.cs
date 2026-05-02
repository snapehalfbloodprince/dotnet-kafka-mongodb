using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace KafkaRouter.Worker.MongoDb.Documents;

public sealed class RoutingRuleDocument
{
    [BsonId]
    [BsonRepresentation(BsonType.ObjectId)]
    public string? Id { get; init; }

    [BsonElement("eventType")]
    public string EventType { get; init; } = string.Empty;

    [BsonElement("destinationTopics")]
    public string[] DestinationTopics { get; init; } = Array.Empty<string>();

    [BsonElement("isEnabled")]
    public bool IsEnabled { get; init; }

    [BsonElement("createdAt")]
    public DateTimeOffset CreatedAt { get; init; }

    [BsonElement("updatedAt")]
    public DateTimeOffset UpdatedAt { get; init; }
}