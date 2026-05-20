using Testcontainers.Kafka;
using Testcontainers.MongoDb;

namespace KafkaRouter.Worker.IntegrationTests.EndToEnd.Infrastructure;

public sealed class KafkaMongoDbTestcontainerFixture : IAsyncLifetime
{
    private readonly KafkaContainer _kafkaContainer = new KafkaBuilder()
        .WithImage("apache/kafka-native:3.8.0")
        .Build();

    private readonly MongoDbContainer _mongoDbContainer = new MongoDbBuilder()
        .WithImage("mongo:8.0")
        .WithUsername("root")
        .WithPassword("rootpassword")
        .Build();

    public string KafkaBootstrapServers => _kafkaContainer.GetBootstrapAddress();

    public string MongoDbConnectionString => _mongoDbContainer.GetConnectionString();

    public string DatabaseName { get; } = $"kafka_router_e2e_{Guid.NewGuid():N}";

    public async Task InitializeAsync()
    {
        await _kafkaContainer.StartAsync();
        await _mongoDbContainer.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _mongoDbContainer.DisposeAsync();
        await _kafkaContainer.DisposeAsync();
    }
}