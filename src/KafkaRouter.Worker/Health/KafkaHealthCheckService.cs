using Confluent.Kafka;
using KafkaRouter.Worker.Options;
using Microsoft.Extensions.Options;

namespace KafkaRouter.Worker.Health;

public sealed class KafkaHealthCheckService : IKafkaHealthCheckService
{
    private readonly KafkaOptions _kafkaOptions;
    private readonly ILogger<KafkaHealthCheckService> _logger;

    public KafkaHealthCheckService(
        IOptions<KafkaOptions> kafkaOptions,
        ILogger<KafkaHealthCheckService> logger)
    {
        _kafkaOptions = kafkaOptions.Value;
        _logger = logger;
    }

    public Task<bool> IsHealthyAsync(CancellationToken cancellationToken)
    {
        try
        {
            using var adminClient = new AdminClientBuilder(
                new AdminClientConfig
                {
                    BootstrapServers = _kafkaOptions.BootstrapServers,
                    SocketTimeoutMs = 3000
                })
                .Build();

            var metadata = adminClient.GetMetadata(
                _kafkaOptions.InputTopic,
                TimeSpan.FromSeconds(3));

            var topic = metadata.Topics.FirstOrDefault(
                topicMetadata => topicMetadata.Topic == _kafkaOptions.InputTopic);

            if (topic is null)
            {
                _logger.LogWarning(
                    "Health check Kafka fallito. Topic non trovato: {InputTopic}.",
                    _kafkaOptions.InputTopic);

                return Task.FromResult(false);
            }

            if (topic.Error.IsError)
            {
                _logger.LogWarning(
                    "Health check Kafka fallito. Topic: {InputTopic}. Error: {Error}.",
                    _kafkaOptions.InputTopic,
                    topic.Error.Reason);

                return Task.FromResult(false);
            }

            return Task.FromResult(true);
        }
        catch (Exception exception)
        {
            _logger.LogWarning(
                exception,
                "Health check Kafka fallito. BootstrapServers: {BootstrapServers}.",
                _kafkaOptions.BootstrapServers);

            return Task.FromResult(false);
        }
    }
}