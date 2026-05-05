using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace KafkaRouter.Worker.MongoDb.Documents;

public sealed class ProcessedMessageDocument
{
    [BsonId]
    [BsonRepresentation(BsonType.ObjectId)]
    public string? Id { get; init; }

    [BsonElement("eventId")]
    public string EventId { get; init; } = string.Empty;

    [BsonElement("eventType")]
    public string EventType { get; init; } = string.Empty;

    [BsonElement("sourceTopic")]
    public string SourceTopic { get; init; } = string.Empty;

    [BsonElement("sourcePartition")]
    public int SourcePartition { get; init; }

    [BsonElement("sourceOffset")]
    public long SourceOffset { get; init; }

    [BsonElement("destinationTopics")]
    public string[] DestinationTopics { get; init; } = Array.Empty<string>();

    [BsonElement("processedAt")]
    public DateTimeOffset ProcessedAt { get; init; }
}