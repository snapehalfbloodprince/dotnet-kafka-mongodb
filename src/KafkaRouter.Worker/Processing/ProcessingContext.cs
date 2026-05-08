using Confluent.Kafka;
using KafkaRouter.Worker.Models;

namespace KafkaRouter.Worker.Processing;

public sealed class ProcessingContext
{
    private ProcessingContext(
        string sourceTopic,
        int sourcePartition,
        long sourceOffset,
        string? messageKey,
        string? eventId,
        string? eventType,
        string? correlationId)
    {
        SourceTopic = sourceTopic;
        SourcePartition = sourcePartition;
        SourceOffset = sourceOffset;
        MessageKey = messageKey;
        EventId = eventId;
        EventType = eventType;
        CorrelationId = correlationId;
    }

    public string SourceTopic { get; }

    public int SourcePartition { get; }

    public long SourceOffset { get; }

    public string? MessageKey { get; }

    public string? EventId { get; }

    public string? EventType { get; }

    public string? CorrelationId { get; }

    public static ProcessingContext FromConsumeResult(
        ConsumeResult<string, string> consumeResult)
    {
        return new ProcessingContext(
            sourceTopic: consumeResult.Topic,
            sourcePartition: consumeResult.Partition.Value,
            sourceOffset: consumeResult.Offset.Value,
            messageKey: consumeResult.Message.Key,
            eventId: null,
            eventType: null,
            correlationId: null);
    }

    public ProcessingContext WithEventEnvelope(EventEnvelope eventEnvelope)
    {
        return new ProcessingContext(
            SourceTopic,
            SourcePartition,
            SourceOffset,
            MessageKey,
            eventEnvelope.EventId,
            eventEnvelope.EventType,
            eventEnvelope.CorrelationId);
    }

    public string GetEffectiveCorrelationId()
    {
        if (!string.IsNullOrWhiteSpace(CorrelationId))
        {
            return CorrelationId;
        }

        if (!string.IsNullOrWhiteSpace(EventId))
        {
            return EventId;
        }

        return $"{SourceTopic}-{SourcePartition}-{SourceOffset}";
    }
}