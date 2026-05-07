using Confluent.Kafka;

namespace KafkaRouter.Worker.Processing;

public interface IMessageProcessingService
{
    Task<MessageProcessingResult> ProcessAsync(
        ConsumeResult<string, string> consumeResult,
        CancellationToken cancellationToken);
}