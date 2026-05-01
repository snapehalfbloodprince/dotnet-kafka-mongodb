using Confluent.Kafka;

namespace KafkaRouter.Worker.Kafka;

public interface IKafkaMessageProducer : IDisposable
{
    Task<DeliveryResult<string, string>> ProduceAsync(
        string topic,
        string? key,
        string value,
        CancellationToken cancellationToken);
}