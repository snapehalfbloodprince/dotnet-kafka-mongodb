using Testcontainers.Kafka;

namespace KafkaRouter.Worker.IntegrationTests.Kafka.Infrastructure;

public sealed class KafkaTestcontainerFixture : IAsyncLifetime
{
    private readonly KafkaContainer _kafkaContainer = new KafkaBuilder()
        .WithImage("apache/kafka-native:3.8.0")
        .Build();

    public string BootstrapServers => _kafkaContainer.GetBootstrapAddress();

    public async Task InitializeAsync()
    {
        await _kafkaContainer.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _kafkaContainer.DisposeAsync();
    }
}