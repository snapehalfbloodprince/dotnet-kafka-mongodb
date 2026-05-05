using KafkaRouter.Worker.MongoDb.Documents;
using KafkaRouter.Worker.Options;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace KafkaRouter.Worker.MongoDb.Repositories;

public sealed class ProcessedMessageRepository : IProcessedMessageRepository
{
    private readonly ILogger<ProcessedMessageRepository> _logger;
    private readonly IMongoCollection<ProcessedMessageDocument> _collection;

    public ProcessedMessageRepository(
        IOptions<MongoDbOptions> options,
        ILogger<ProcessedMessageRepository> logger)
    {
        _logger = logger;

        var mongoOptions = options.Value;

        var mongoClient = new MongoClient(mongoOptions.ConnectionString);
        var database = mongoClient.GetDatabase(mongoOptions.DatabaseName);

        _collection = database.GetCollection<ProcessedMessageDocument>(
            mongoOptions.ProcessedMessagesCollectionName);
    }

    public async Task EnsureIndexesAsync(CancellationToken cancellationToken)
    {
        var eventIdIndexKeys = Builders<ProcessedMessageDocument>
            .IndexKeys
            .Ascending(message => message.EventId);

        var eventIdIndexModel = new CreateIndexModel<ProcessedMessageDocument>(
            eventIdIndexKeys,
            new CreateIndexOptions
            {
                Name = "ux_processed_messages_event_id",
                Unique = true
            });

        var sourcePositionIndexKeys = Builders<ProcessedMessageDocument>
            .IndexKeys
            .Ascending(message => message.SourceTopic)
            .Ascending(message => message.SourcePartition)
            .Ascending(message => message.SourceOffset);

        var sourcePositionIndexModel = new CreateIndexModel<ProcessedMessageDocument>(
            sourcePositionIndexKeys,
            new CreateIndexOptions
            {
                Name = "ix_processed_messages_source_position",
                Unique = false
            });

        await _collection.Indexes.CreateManyAsync(
            new[]
            {
                eventIdIndexModel,
                sourcePositionIndexModel
            },
            cancellationToken);

        _logger.LogInformation(
            "Indici MongoDB creati/verificati sulla collection processed_messages: {Indexes}.",
            "ux_processed_messages_event_id, ix_processed_messages_source_position");
    }

    public async Task<bool> ExistsByEventIdAsync(
        string eventId,
        CancellationToken cancellationToken)
    {
        var filter = Builders<ProcessedMessageDocument>
            .Filter
            .Eq(message => message.EventId, eventId);

        var count = await _collection
            .CountDocumentsAsync(
                filter,
                cancellationToken: cancellationToken);

        return count > 0;
    }

    public async Task<bool> TryInsertAsync(
        ProcessedMessageDocument processedMessage,
        CancellationToken cancellationToken)
    {
        try
        {
            await _collection.InsertOneAsync(
                processedMessage,
                cancellationToken: cancellationToken);

            _logger.LogInformation(
                "Messaggio registrato come processato. EventId: {EventId}. EventType: {EventType}. SourceTopic: {SourceTopic}. SourcePartition: {SourcePartition}. SourceOffset: {SourceOffset}.",
                processedMessage.EventId,
                processedMessage.EventType,
                processedMessage.SourceTopic,
                processedMessage.SourcePartition,
                processedMessage.SourceOffset);

            return true;
        }
        catch (MongoWriteException exception) when (exception.WriteError.Category == ServerErrorCategory.DuplicateKey)
        {
            _logger.LogWarning(
                "Tentativo di inserire un messaggio già processato. EventId: {EventId}. SourceTopic: {SourceTopic}. SourcePartition: {SourcePartition}. SourceOffset: {SourceOffset}.",
                processedMessage.EventId,
                processedMessage.SourceTopic,
                processedMessage.SourcePartition,
                processedMessage.SourceOffset);

            return false;
        }
    }
}