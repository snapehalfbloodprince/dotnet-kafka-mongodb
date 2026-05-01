using Confluent.Kafka;

namespace KafkaRouter.Worker.Kafka;

public interface IKafkaMessageConsumer : IDisposable
{
    void Subscribe();

    ConsumeResult<string, string> Consume(CancellationToken cancellationToken);

    void Commit(ConsumeResult<string, string> consumeResult);
}