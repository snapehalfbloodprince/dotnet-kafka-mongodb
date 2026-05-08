using FluentAssertions;
using KafkaRouter.Worker.Metrics;
using KafkaRouter.Worker.Options;

namespace KafkaRouter.Worker.UnitTests.Metrics;

public sealed class InMemoryWorkerMetricsTests
{
    [Fact]
    public void GetSnapshot_WhenNoMetricsHaveBeenIncremented_ShouldReturnZeroCounters()
    {
        // Arrange
        var sut = CreateSut("worker-test");

        // Act
        var snapshot = sut.GetSnapshot();

        // Assert
        snapshot.InstanceName.Should().Be("worker-test");
        snapshot.ProcessedMessages.Should().Be(0);
        snapshot.DeadLetterMessages.Should().Be(0);
        snapshot.DuplicateMessages.Should().Be(0);
        snapshot.TechnicalFailures.Should().Be(0);

        snapshot.LastProcessedEventId.Should().BeNull();
        snapshot.LastDeadLetterEventId.Should().BeNull();
        snapshot.LastDuplicateEventId.Should().BeNull();
        snapshot.LastTechnicalFailureCategory.Should().BeNull();

        snapshot.StartedAt.Should().BeOnOrBefore(snapshot.CheckedAt);
    }

    [Fact]
    public void IncrementProcessedMessages_ShouldIncreaseProcessedCounterAndStoreLastProcessedInfo()
    {
        // Arrange
        var sut = CreateSut("worker-test");

        // Act
        sut.IncrementProcessedMessages(
            eventId: "event-001",
            eventType: "CustomerCreated");

        var snapshot = sut.GetSnapshot();

        // Assert
        snapshot.ProcessedMessages.Should().Be(1);
        snapshot.LastProcessedEventId.Should().Be("event-001");
        snapshot.LastProcessedEventType.Should().Be("CustomerCreated");
        snapshot.LastProcessedAt.Should().NotBeNull();
    }

    [Fact]
    public void IncrementDeadLetterMessages_ShouldIncreaseDeadLetterCounterAndStoreLastDeadLetterInfo()
    {
        // Arrange
        var sut = CreateSut("worker-test");

        // Act
        sut.IncrementDeadLetterMessages(
            eventId: "event-001",
            eventType: "UnknownEvent",
            errorCode: "ROUTING_RULE_NOT_FOUND");

        var snapshot = sut.GetSnapshot();

        // Assert
        snapshot.DeadLetterMessages.Should().Be(1);
        snapshot.LastDeadLetterEventId.Should().Be("event-001");
        snapshot.LastDeadLetterEventType.Should().Be("UnknownEvent");
        snapshot.LastDeadLetterErrorCode.Should().Be("ROUTING_RULE_NOT_FOUND");
        snapshot.LastDeadLetterAt.Should().NotBeNull();
    }

    [Fact]
    public void IncrementDuplicateMessages_ShouldIncreaseDuplicateCounterAndStoreLastDuplicateInfo()
    {
        // Arrange
        var sut = CreateSut("worker-test");

        // Act
        sut.IncrementDuplicateMessages(
            eventId: "event-001",
            eventType: "CustomerCreated");

        var snapshot = sut.GetSnapshot();

        // Assert
        snapshot.DuplicateMessages.Should().Be(1);
        snapshot.LastDuplicateEventId.Should().Be("event-001");
        snapshot.LastDuplicateEventType.Should().Be("CustomerCreated");
        snapshot.LastDuplicateAt.Should().NotBeNull();
    }

    [Fact]
    public void IncrementTechnicalFailures_ShouldIncreaseTechnicalFailuresCounterAndStoreLastFailureCategory()
    {
        // Arrange
        var sut = CreateSut("worker-test");

        // Act
        sut.IncrementTechnicalFailures("KAFKA_ERROR");

        var snapshot = sut.GetSnapshot();

        // Assert
        snapshot.TechnicalFailures.Should().Be(1);
        snapshot.LastTechnicalFailureCategory.Should().Be("KAFKA_ERROR");
        snapshot.LastTechnicalFailureAt.Should().NotBeNull();
    }

    [Fact]
    public void MultipleIncrements_ShouldReturnExpectedCounters()
    {
        // Arrange
        var sut = CreateSut("worker-test");

        // Act
        sut.IncrementProcessedMessages("event-001", "CustomerCreated");
        sut.IncrementProcessedMessages("event-002", "InvoicePaid");
        sut.IncrementDeadLetterMessages("event-003", "UnknownEvent", "ROUTING_RULE_NOT_FOUND");
        sut.IncrementDuplicateMessages("event-001", "CustomerCreated");
        sut.IncrementTechnicalFailures("KAFKA_ERROR");
        sut.IncrementTechnicalFailures("MONGO_ERROR");

        var snapshot = sut.GetSnapshot();

        // Assert
        snapshot.ProcessedMessages.Should().Be(2);
        snapshot.DeadLetterMessages.Should().Be(1);
        snapshot.DuplicateMessages.Should().Be(1);
        snapshot.TechnicalFailures.Should().Be(2);

        snapshot.LastProcessedEventId.Should().Be("event-002");
        snapshot.LastTechnicalFailureCategory.Should().Be("MONGO_ERROR");
    }

    private static InMemoryWorkerMetrics CreateSut(string instanceName)
    {
        var options = Microsoft.Extensions.Options.Options.Create(new WorkerOptions
        {
            InstanceName = instanceName
        });

        return new InMemoryWorkerMetrics(options);
    }
}