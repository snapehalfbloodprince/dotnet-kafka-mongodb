namespace KafkaRouter.Worker.Health;

public interface IKafkaHealthCheckService
{
    Task<bool> IsHealthyAsync(CancellationToken cancellationToken);
}