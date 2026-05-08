using FluentAssertions;
using KafkaRouter.Worker.Options;
using KafkaRouter.Worker.Options.Validation;

namespace KafkaRouter.Worker.UnitTests.Options;

public sealed class KafkaOptionsValidatorTests
{
    private readonly KafkaOptionsValidator _sut = new();

    [Fact]
    public void Validate_WhenOptionsAreValid_ShouldReturnSuccess()
    {
        // Arrange
        var options = new KafkaOptions
        {
            BootstrapServers = "localhost:9092",
            InputTopic = "events.inbound",
            DeadLetterTopic = "events.dead-letter",
            ConsumerGroupId = "kafka-router-worker-local",
            AutoOffsetReset = "Earliest"
        };

        // Act
        var result = _sut.Validate(null, options);

        // Assert
        result.Succeeded.Should().BeTrue();
    }

    [Fact]
    public void Validate_WhenAutoOffsetResetIsInvalid_ShouldReturnFailure()
    {
        // Arrange
        var options = new KafkaOptions
        {
            BootstrapServers = "localhost:9092",
            InputTopic = "events.inbound",
            DeadLetterTopic = "events.dead-letter",
            ConsumerGroupId = "kafka-router-worker-local",
            AutoOffsetReset = "banana"
        };

        // Act
        var result = _sut.Validate(null, options);

        // Assert
        result.Failed.Should().BeTrue();
        result.Failures.Should().Contain("Kafka:AutoOffsetReset deve essere Earliest, Latest oppure Error.");
    }

    [Fact]
    public void Validate_WhenInputTopicAndDeadLetterTopicAreEqual_ShouldReturnFailure()
    {
        // Arrange
        var options = new KafkaOptions
        {
            BootstrapServers = "localhost:9092",
            InputTopic = "events.inbound",
            DeadLetterTopic = " EVENTS.INBOUND ",
            ConsumerGroupId = "kafka-router-worker-local",
            AutoOffsetReset = "Earliest"
        };

        // Act
        var result = _sut.Validate(null, options);

        // Assert
        result.Failed.Should().BeTrue();
        result.Failures.Should().Contain("Kafka:InputTopic e Kafka:DeadLetterTopic non possono coincidere.");
    }

    [Fact]
    public void Validate_WhenRequiredValuesAreMissing_ShouldReturnFailures()
    {
        // Arrange
        var options = new KafkaOptions
        {
            BootstrapServers = "",
            InputTopic = "",
            DeadLetterTopic = "",
            ConsumerGroupId = "",
            AutoOffsetReset = ""
        };

        // Act
        var result = _sut.Validate(null, options);

        // Assert
        result.Failed.Should().BeTrue();
        result.Failures.Should().Contain(error => error.Contains("BootstrapServers"));
        result.Failures.Should().Contain(error => error.Contains("InputTopic"));
        result.Failures.Should().Contain(error => error.Contains("DeadLetterTopic"));
        result.Failures.Should().Contain(error => error.Contains("ConsumerGroupId"));
        result.Failures.Should().Contain(error => error.Contains("AutoOffsetReset"));
    }
}