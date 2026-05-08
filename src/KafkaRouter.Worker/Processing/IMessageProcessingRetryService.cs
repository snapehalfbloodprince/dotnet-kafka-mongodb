using Confluent.Kafka;

namespace KafkaRouter.Worker.Processing;

public interface IMessageProcessingRetryService
{
    Task<MessageProcessingResult> ProcessWithRetryAsync(
        ConsumeResult<string, string> consumeResult,
        CancellationToken cancellationToken);
}