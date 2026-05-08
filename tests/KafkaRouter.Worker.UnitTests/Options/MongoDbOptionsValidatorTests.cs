using FluentAssertions;
using KafkaRouter.Worker.Options;
using KafkaRouter.Worker.Options.Validation;

namespace KafkaRouter.Worker.UnitTests.Options;

public sealed class MongoDbOptionsValidatorTests
{
    private readonly MongoDbOptionsValidator _sut = new();

    [Fact]
    public void Validate_WhenOptionsAreValid_ShouldReturnSuccess()
    {
        // Arrange
        var options = new MongoDbOptions
        {
            ConnectionString = "mongodb://root:rootpassword@localhost:27017",
            DatabaseName = "kafka_router",
            RoutingRulesCollectionName = "routing_rules",
            ProcessedMessagesCollectionName = "processed_messages"
        };

        // Act
        var result = _sut.Validate(null, options);

        // Assert
        result.Succeeded.Should().BeTrue();
    }

    [Fact]
    public void Validate_WhenConnectionStringIsInvalid_ShouldReturnFailure()
    {
        // Arrange
        var options = new MongoDbOptions
        {
            ConnectionString = "not-a-mongodb-uri",
            DatabaseName = "kafka_router",
            RoutingRulesCollectionName = "routing_rules",
            ProcessedMessagesCollectionName = "processed_messages"
        };

        // Act
        var result = _sut.Validate(null, options);

        // Assert
        result.Failed.Should().BeTrue();
        result.Failures.Should().Contain("MongoDb:ConnectionString deve essere una URI MongoDB valida.");
    }

    [Fact]
    public void Validate_WhenCollectionsAreEqual_ShouldReturnFailure()
    {
        // Arrange
        var options = new MongoDbOptions
        {
            ConnectionString = "mongodb://root:rootpassword@localhost:27017",
            DatabaseName = "kafka_router",
            RoutingRulesCollectionName = "routing_rules",
            ProcessedMessagesCollectionName = " ROUTING_RULES "
        };

        // Act
        var result = _sut.Validate(null, options);

        // Assert
        result.Failed.Should().BeTrue();
        result.Failures.Should().Contain("MongoDb:RoutingRulesCollectionName e MongoDb:ProcessedMessagesCollectionName non possono coincidere.");
    }

    [Fact]
    public void Validate_WhenRequiredValuesAreMissing_ShouldReturnFailures()
    {
        // Arrange
        var options = new MongoDbOptions
        {
            ConnectionString = "",
            DatabaseName = "",
            RoutingRulesCollectionName = "",
            ProcessedMessagesCollectionName = ""
        };

        // Act
        var result = _sut.Validate(null, options);

        // Assert
        result.Failed.Should().BeTrue();
        result.Failures.Should().Contain(error => error.Contains("ConnectionString"));
        result.Failures.Should().Contain(error => error.Contains("DatabaseName"));
        result.Failures.Should().Contain(error => error.Contains("RoutingRulesCollectionName"));
        result.Failures.Should().Contain(error => error.Contains("ProcessedMessagesCollectionName"));
    }
}