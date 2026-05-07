namespace KafkaRouter.Worker.Health;

public interface IMongoDbHealthCheckService
{
    Task<bool> IsHealthyAsync(CancellationToken cancellationToken);
}