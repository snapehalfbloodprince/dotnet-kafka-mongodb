using Confluent.Kafka;

namespace KafkaRouter.Worker.Processing;

public interface IMessageProcessingService
{
    Task ProcessAsync(
        ConsumeResult<string, string> consumeResult,
        CancellationToken cancellationToken);
}