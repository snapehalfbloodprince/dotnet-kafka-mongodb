using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace KafkaRouter.Worker.IntegrationTests.Kafka.Infrastructure;

public static class KafkaTestClient
{
    public static async Task CreateTopicAsync(
        string bootstrapServers,
        string topicName,
        int partitions = 1,
        short replicationFactor = 1,
        CancellationToken cancellationToken = default)
    {
        using var adminClient = new AdminClientBuilder(
            new AdminClientConfig
            {
                BootstrapServers = bootstrapServers
            })
            .Build();

        try
        {
            await adminClient.CreateTopicsAsync(
                new[]
                {
                    new TopicSpecification
                    {
                        Name = topicName,
                        NumPartitions = partitions,
                        ReplicationFactor = replicationFactor
                    }
                });
        }
        catch (CreateTopicsException exception)
        {
            var onlyAlreadyExists = exception.Results.All(result =>
                result.Error.Code == ErrorCode.TopicAlreadyExists);

            if (!onlyAlreadyExists)
            {
                throw;
            }
        }
    }

    public static async Task ProduceAsync(
        string bootstrapServers,
        string topic,
        string key,
        string value,
        CancellationToken cancellationToken = default)
    {
        using var producer = new ProducerBuilder<string, string>(
            new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                Acks = Acks.All
            })
            .Build();

        await producer.ProduceAsync(
            topic,
            new Message<string, string>
            {
                Key = key,
                Value = value
            },
            cancellationToken);

        producer.Flush(TimeSpan.FromSeconds(5));
    }

    public static string ConsumeSingleValue(
        string bootstrapServers,
        string topic,
        string groupId,
        TimeSpan timeout)
    {
        using var consumer = new ConsumerBuilder<string, string>(
            new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            })
            .Build();

        consumer.Subscribe(topic);

        var deadline = DateTimeOffset.UtcNow.Add(timeout);

        while (DateTimeOffset.UtcNow < deadline)
        {
            var result = consumer.Consume(TimeSpan.FromMilliseconds(250));

            if (result is not null)
            {
                return result.Message.Value;
            }
        }

        throw new TimeoutException(
            $"Nessun messaggio ricevuto dal topic '{topic}' entro {timeout.TotalSeconds} secondi.");
    }
}