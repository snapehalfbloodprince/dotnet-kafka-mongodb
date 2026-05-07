using Confluent.Kafka;
using KafkaRouter.Worker.Options;
using Microsoft.Extensions.Options;

namespace KafkaRouter.Worker.Kafka;

public sealed class KafkaMessageConsumer : IKafkaMessageConsumer
{
    private readonly ILogger<KafkaMessageConsumer> _logger;
    private readonly KafkaOptions _kafkaOptions;
    private readonly WorkerOptions _workerOptions;
    private readonly IConsumer<string, string> _consumer;

    private bool _isSubscribed;

    public KafkaMessageConsumer(
        IOptions<KafkaOptions> kafkaOptions,
        IOptions<WorkerOptions> workerOptions,
        ILogger<KafkaMessageConsumer> logger)
    {
        _kafkaOptions = kafkaOptions.Value;
        _workerOptions = workerOptions.Value;
        _logger = logger;

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _kafkaOptions.BootstrapServers,
            GroupId = _kafkaOptions.ConsumerGroupId,
            AutoOffsetReset = ParseAutoOffsetReset(_kafkaOptions.AutoOffsetReset),
            EnableAutoCommit = false,
            ClientId = $"kafka-router-consumer-{_workerOptions.InstanceName}"
        };

        _consumer = new ConsumerBuilder<string, string>(consumerConfig)
            .SetErrorHandler((_, error) =>
            {
                _logger.LogError(
                    "Errore Kafka. InstanceName: {InstanceName}. Code: {Code}. Reason: {Reason}. IsFatal: {IsFatal}",
                    _workerOptions.InstanceName,
                    error.Code,
                    error.Reason,
                    error.IsFatal);
            })
            .SetPartitionsAssignedHandler((_, partitions) =>
            {
                _logger.LogInformation(
                    "Partizioni assegnate al consumer. InstanceName: {InstanceName}. Partitions: {Partitions}.",
                    _workerOptions.InstanceName,
                    string.Join(", ", partitions.Select(partition => partition.ToString())));
            })
            .SetPartitionsRevokedHandler((_, partitions) =>
            {
                _logger.LogWarning(
                    "Partizioni revocate al consumer. InstanceName: {InstanceName}. Partitions: {Partitions}.",
                    _workerOptions.InstanceName,
                    string.Join(", ", partitions.Select(partition => partition.ToString())));
            })
            .Build();
    }

    public void Subscribe()
    {
        if (_isSubscribed)
        {
            return;
        }

        _consumer.Subscribe(_kafkaOptions.InputTopic);
        _isSubscribed = true;

        _logger.LogInformation(
            "Consumer Kafka sottoscritto. InstanceName: {InstanceName}. InputTopic: {InputTopic}. ConsumerGroupId: {ConsumerGroupId}.",
            _workerOptions.InstanceName,
            _kafkaOptions.InputTopic,
            _kafkaOptions.ConsumerGroupId);
    }

    public ConsumeResult<string, string> Consume(CancellationToken cancellationToken)
    {
        if (!_isSubscribed)
        {
            Subscribe();
        }

        return _consumer.Consume(cancellationToken);
    }

    public void Commit(ConsumeResult<string, string> consumeResult)
    {
        _consumer.Commit(consumeResult);

        _logger.LogInformation(
            "Offset committato. InstanceName: {InstanceName}. Topic: {Topic}. Partition: {Partition}. Offset: {Offset}.",
            _workerOptions.InstanceName,
            consumeResult.Topic,
            consumeResult.Partition.Value,
            consumeResult.Offset.Value);
    }

    public void Dispose()
    {
        try
        {
            _logger.LogInformation(
                "Chiusura consumer Kafka in corso. InstanceName: {InstanceName}.",
                _workerOptions.InstanceName);

            _consumer.Close();

            _logger.LogInformation(
                "Consumer Kafka chiuso correttamente. InstanceName: {InstanceName}.",
                _workerOptions.InstanceName);
        }
        catch (KafkaException exception)
        {
            _logger.LogError(
                exception,
                "Errore durante la chiusura del consumer Kafka. InstanceName: {InstanceName}.",
                _workerOptions.InstanceName);
        }
        finally
        {
            _consumer.Dispose();
        }
    }

    private static AutoOffsetReset ParseAutoOffsetReset(string value)
    {
        return value.Trim().ToLowerInvariant() switch
        {
            "earliest" => AutoOffsetReset.Earliest,
            "latest" => AutoOffsetReset.Latest,
            "error" => AutoOffsetReset.Error,
            _ => AutoOffsetReset.Earliest
        };
    }
}