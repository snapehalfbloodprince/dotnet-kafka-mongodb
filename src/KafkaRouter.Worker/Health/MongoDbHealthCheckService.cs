using KafkaRouter.Worker.Options;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;

namespace KafkaRouter.Worker.Health;

public sealed class MongoDbHealthCheckService : IMongoDbHealthCheckService
{
    private readonly MongoDbOptions _mongoDbOptions;
    private readonly ILogger<MongoDbHealthCheckService> _logger;

    public MongoDbHealthCheckService(
        IOptions<MongoDbOptions> mongoDbOptions,
        ILogger<MongoDbHealthCheckService> logger)
    {
        _mongoDbOptions = mongoDbOptions.Value;
        _logger = logger;
    }

    public async Task<bool> IsHealthyAsync(CancellationToken cancellationToken)
    {
        try
        {
            var mongoClient = new MongoClient(_mongoDbOptions.ConnectionString);
            var database = mongoClient.GetDatabase(_mongoDbOptions.DatabaseName);

            using var timeoutCancellationTokenSource = CancellationTokenSource
                .CreateLinkedTokenSource(cancellationToken);

            timeoutCancellationTokenSource.CancelAfter(TimeSpan.FromSeconds(3));

            var command = new BsonDocument("ping", 1);

            await database.RunCommandAsync<BsonDocument>(
                command,
                cancellationToken: timeoutCancellationTokenSource.Token);

            return true;
        }
        catch (Exception exception)
        {
            _logger.LogWarning(
                exception,
                "Health check MongoDB fallito. DatabaseName: {DatabaseName}.",
                _mongoDbOptions.DatabaseName);

            return false;
        }
    }
}