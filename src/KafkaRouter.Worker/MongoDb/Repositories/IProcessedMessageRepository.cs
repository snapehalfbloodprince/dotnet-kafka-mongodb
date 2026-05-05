using KafkaRouter.Worker.MongoDb.Documents;

namespace KafkaRouter.Worker.MongoDb.Repositories;

public interface IProcessedMessageRepository
{
    Task EnsureIndexesAsync(CancellationToken cancellationToken);

    Task<bool> ExistsByEventIdAsync(
        string eventId,
        CancellationToken cancellationToken);

    Task<bool> TryInsertAsync(
        ProcessedMessageDocument processedMessage,
        CancellationToken cancellationToken);
}