using System.Text.Json;
using Confluent.Kafka;
using KafkaRouter.Worker.Models;

namespace KafkaRouter.Worker.DeadLetter;

public sealed class DeadLetterMessageFactory : IDeadLetterMessageFactory
{
    private static readonly JsonSerializerOptions JsonSerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    public string CreateDeadLetterPayload(
        ConsumeResult<string, string> consumeResult,
        string errorCode,
        string errorMessage,
        EventEnvelope? eventEnvelope = null)
    {
        var deadLetterMessage = new DeadLetterMessage
        {
            OriginalTopic = consumeResult.Topic,
            OriginalPartition = consumeResult.Partition.Value,
            OriginalOffset = consumeResult.Offset.Value,
            OriginalKey = consumeResult.Message.Key,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage,
            FailedAt = DateTimeOffset.UtcNow,
            EventId = eventEnvelope?.EventId,
            EventType = eventEnvelope?.EventType,
            CorrelationId = eventEnvelope?.CorrelationId,
            OriginalPayload = consumeResult.Message.Value
        };

        return JsonSerializer.Serialize(
            deadLetterMessage,
            JsonSerializerOptions);
    }
}