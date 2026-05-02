namespace KafkaRouter.Worker.Startup;

public interface IMongoDbInitializer
{
    Task InitializeAsync(CancellationToken cancellationToken);
}