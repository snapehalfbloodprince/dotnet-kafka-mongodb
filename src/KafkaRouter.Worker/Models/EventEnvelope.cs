using System.Text.Json;
using System.Text.Json.Serialization;

namespace KafkaRouter.Worker.Models;

public sealed class EventEnvelope
{
    [JsonPropertyName("eventId")]
    public string? EventId { get; init; }

    [JsonPropertyName("eventType")]
    public string? EventType { get; init; }

    [JsonPropertyName("occurredAt")]
    public DateTimeOffset? OccurredAt { get; init; }

    [JsonPropertyName("source")]
    public string? Source { get; init; }

    [JsonPropertyName("correlationId")]
    public string? CorrelationId { get; init; }

    [JsonPropertyName("payload")]
    public JsonElement Payload { get; init; }
}