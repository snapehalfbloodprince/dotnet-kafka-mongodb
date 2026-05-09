using Confluent.Kafka;
using KafkaRouter.Worker.Options;
using Microsoft.Extensions.Options;

namespace KafkaRouter.Worker.Kafka;

public sealed class KafkaMessageProducer : IKafkaMessageProducer
{
    private readonly ILogger<KafkaMessageProducer> _logger;
    private readonly WorkerOptions _workerOptions;
    private readonly IProducer<string, string> _producer;

    public KafkaMessageProducer(
        IOptions<KafkaOptions> kafkaOptions,
        IOptions<WorkerOptions> workerOptions,
        ILogger<KafkaMessageProducer> logger)
    {
        _logger = logger;
        _workerOptions = workerOptions.Value;

        var options = kafkaOptions.Value;

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
            ClientId = $"kafka-router-producer-{_workerOptions.InstanceName}"
        };

        _producer = new ProducerBuilder<string, string>(producerConfig)
            .SetErrorHandler((_, error) =>
            {
                _logger.LogError(
                    "Errore Kafka producer. InstanceName: {InstanceName}. Code: {Code}. Reason: {Reason}. IsFatal: {IsFatal}",
                    _workerOptions.InstanceName,
                    error.Code,
                    error.Reason,
                    error.IsFatal);
            })
            .Build();
    }

    public async Task<DeliveryResult<string, string>> ProduceAsync(
        string topic,
        string? key,
        string value,
        CancellationToken cancellationToken)
    {
        var message = new Message<string, string>
        {
            Value = value
        };

        if (key is not null)
        {
            message.Key = key;
        }

        var deliveryResult = await _producer.ProduceAsync(
            topic,
            message,
            cancellationToken);

        _logger.LogInformation(
            "Messaggio prodotto su Kafka. InstanceName: {InstanceName}. Topic: {Topic}. Partition: {Partition}. Offset: {Offset}. Status: {Status}.",
            _workerOptions.InstanceName,
            deliveryResult.Topic,
            deliveryResult.Partition.Value,
            deliveryResult.Offset.Value,
            deliveryResult.Status);

        return deliveryResult;
    }

    public void Dispose()
    {
        try
        {
            _logger.LogInformation(
                "Flush producer Kafka in corso. InstanceName: {InstanceName}.",
                _workerOptions.InstanceName);

            _producer.Flush(TimeSpan.FromSeconds(10));

            _logger.LogInformation(
                "Producer Kafka flush completato. InstanceName: {InstanceName}.",
                _workerOptions.InstanceName);
        }
        catch (KafkaException exception)
        {
            _logger.LogError(
                exception,
                "Errore durante il flush del producer Kafka. InstanceName: {InstanceName}.",
                _workerOptions.InstanceName);
        }
        finally
        {
            _producer.Dispose();
        }
    }
}