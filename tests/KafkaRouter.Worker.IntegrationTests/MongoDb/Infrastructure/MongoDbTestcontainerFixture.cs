using Testcontainers.MongoDb;

namespace KafkaRouter.Worker.IntegrationTests.MongoDb.Infrastructure;

public sealed class MongoDbTestcontainerFixture : IAsyncLifetime
{
    private readonly MongoDbContainer _mongoDbContainer = new MongoDbBuilder()
        .WithImage("mongo:8.0")
        .WithUsername("root")
        .WithPassword("rootpassword")
        .Build();

    public string ConnectionString => _mongoDbContainer.GetConnectionString();

    public string DatabaseName { get; } = $"kafka_router_tests_{Guid.NewGuid():N}";

    public async Task InitializeAsync()
    {
        await _mongoDbContainer.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _mongoDbContainer.DisposeAsync();
    }
}