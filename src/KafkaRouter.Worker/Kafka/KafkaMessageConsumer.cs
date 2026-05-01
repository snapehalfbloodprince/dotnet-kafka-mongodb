using Confluent.Kafka;
using KafkaRouter.Worker.Options;
using Microsoft.Extensions.Options;

namespace KafkaRouter.Worker.Kafka;

public sealed class KafkaMessageConsumer : IKafkaMessageConsumer
{
    private readonly ILogger<KafkaMessageConsumer> _logger;
    private readonly KafkaOptions _options;
    private readonly IConsumer<string, string> _consumer;

    private bool _isSubscribed;

    public KafkaMessageConsumer(
        IOptions<KafkaOptions> options,
        ILogger<KafkaMessageConsumer> logger)
    {
        _options = options.Value;
        _logger = logger;

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = _options.ConsumerGroupId,
            AutoOffsetReset = ParseAutoOffsetReset(_options.AutoOffsetReset),

            // Best practice didattica:
            // non vogliamo che Kafka committi automaticamente.
            // Il commit deve avvenire solo dopo che il messaggio è stato processato.
            EnableAutoCommit = false,

            ClientId = "kafka-router-worker"
        };

        _consumer = new ConsumerBuilder<string, string>(consumerConfig)
            .SetErrorHandler((_, error) =>
            {
                _logger.LogError(
                    "Errore Kafka. Code: {Code}. Reason: {Reason}. IsFatal: {IsFatal}",
                    error.Code,
                    error.Reason,
                    error.IsFatal);
            })
            .SetPartitionsAssignedHandler((_, partitions) =>
            {
                _logger.LogInformation(
                    "Partizioni assegnate al consumer: {Partitions}",
                    string.Join(", ", partitions.Select(partition => partition.ToString())));
            })
            .SetPartitionsRevokedHandler((_, partitions) =>
            {
                _logger.LogWarning(
                    "Partizioni revocate al consumer: {Partitions}",
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

        _consumer.Subscribe(_options.InputTopic);
        _isSubscribed = true;

        _logger.LogInformation(
            "Consumer Kafka sottoscritto al topic {InputTopic} con consumer group {ConsumerGroupId}.",
            _options.InputTopic,
            _options.ConsumerGroupId);
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
            "Offset committato. Topic: {Topic}. Partition: {Partition}. Offset: {Offset}.",
            consumeResult.Topic,
            consumeResult.Partition.Value,
            consumeResult.Offset.Value);
    }

    public void Dispose()
    {
        try
        {
            _logger.LogInformation("Chiusura consumer Kafka in corso.");
            _consumer.Close();
            _logger.LogInformation("Consumer Kafka chiuso correttamente.");
        }
        catch (KafkaException exception)
        {
            _logger.LogError(
                exception,
                "Errore durante la chiusura del consumer Kafka.");
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