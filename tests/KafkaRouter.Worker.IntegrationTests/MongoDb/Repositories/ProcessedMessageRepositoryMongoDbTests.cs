using FluentAssertions;
using KafkaRouter.Worker.IntegrationTests.MongoDb.Infrastructure;
using KafkaRouter.Worker.MongoDb.Documents;

namespace KafkaRouter.Worker.IntegrationTests.MongoDb.Repositories;

public sealed class ProcessedMessageRepositoryMongoDbTests
    : IClassFixture<MongoDbTestcontainerFixture>
{
    private readonly MongoDbTestcontainerFixture _fixture;

    public ProcessedMessageRepositoryMongoDbTests(
        MongoDbTestcontainerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task TryInsertAsync_WhenMessageDoesNotExist_ShouldInsertAndReturnTrue()
    {
        // Arrange
        var repository = MongoDbRepositoryFactory.CreateProcessedMessageRepository(
            _fixture);

        await repository.EnsureIndexesAsync(CancellationToken.None);

        var document = CreateProcessedMessageDocument(
            eventId: $"event-{Guid.NewGuid():N}");

        // Act
        var inserted = await repository.TryInsertAsync(
            document,
            CancellationToken.None);

        var exists = await repository.ExistsByEventIdAsync(
            document.EventId,
            CancellationToken.None);

        // Assert
        inserted.Should().BeTrue();
        exists.Should().BeTrue();
    }

    [Fact]
    public async Task TryInsertAsync_WhenMessageAlreadyExists_ShouldReturnFalse()
    {
        // Arrange
        var repository = MongoDbRepositoryFactory.CreateProcessedMessageRepository(
            _fixture);

        await repository.EnsureIndexesAsync(CancellationToken.None);

        var eventId = $"event-{Guid.NewGuid():N}";

        var firstDocument = CreateProcessedMessageDocument(eventId);
        var secondDocument = CreateProcessedMessageDocument(eventId);

        // Act
        var firstInserted = await repository.TryInsertAsync(
            firstDocument,
            CancellationToken.None);

        var secondInserted = await repository.TryInsertAsync(
            secondDocument,
            CancellationToken.None);

        // Assert
        firstInserted.Should().BeTrue();
        secondInserted.Should().BeFalse();
    }

    [Fact]
    public async Task ExistsByEventIdAsync_WhenMessageDoesNotExist_ShouldReturnFalse()
    {
        // Arrange
        var repository = MongoDbRepositoryFactory.CreateProcessedMessageRepository(
            _fixture);

        await repository.EnsureIndexesAsync(CancellationToken.None);

        // Act
        var exists = await repository.ExistsByEventIdAsync(
            $"missing-{Guid.NewGuid():N}",
            CancellationToken.None);

        // Assert
        exists.Should().BeFalse();
    }

    private static ProcessedMessageDocument CreateProcessedMessageDocument(
        string eventId)
    {
        return new ProcessedMessageDocument
        {
            EventId = eventId,
            EventType = "CustomerCreated",
            SourceTopic = "events.inbound",
            SourcePartition = 0,
            SourceOffset = 1,
            DestinationTopics = new[] { "events.crm" },
            ProcessedAt = DateTimeOffset.UtcNow
        };
    }
}