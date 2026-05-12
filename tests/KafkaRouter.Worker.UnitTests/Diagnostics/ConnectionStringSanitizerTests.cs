using FluentAssertions;
using KafkaRouter.Worker.Diagnostics;

namespace KafkaRouter.Worker.UnitTests.Diagnostics;

public sealed class ConnectionStringSanitizerTests
{
    [Fact]
    public void SanitizeMongoDbConnectionString_WhenConnectionStringContainsCredentials_ShouldMaskCredentials()
    {
        // Arrange
        var connectionString = "mongodb://root:rootpassword@mongodb:27017";

        // Act
        var result = ConnectionStringSanitizer.SanitizeMongoDbConnectionString(
            connectionString);

        // Assert
        result.Should().Be("mongodb://***:***@mongodb:27017");
        result.Should().NotContain("root");
        result.Should().NotContain("rootpassword");
    }

    [Fact]
    public void SanitizeMongoDbConnectionString_WhenConnectionStringDoesNotContainCredentials_ShouldReturnOriginalValue()
    {
        // Arrange
        var connectionString = "mongodb://mongodb:27017";

        // Act
        var result = ConnectionStringSanitizer.SanitizeMongoDbConnectionString(
            connectionString);

        // Assert
        result.Should().Be(connectionString);
    }

    [Fact]
    public void SanitizeMongoDbConnectionString_WhenConnectionStringIsEmpty_ShouldReturnEmptyString()
    {
        // Act
        var result = ConnectionStringSanitizer.SanitizeMongoDbConnectionString(
            string.Empty);

        // Assert
        result.Should().BeEmpty();
    }

    [Fact]
    public void SanitizeMongoDbConnectionString_WhenConnectionStringIsInvalid_ShouldReturnMaskedValue()
    {
        // Arrange
        var connectionString = "not-a-valid-connection-string";

        // Act
        var result = ConnectionStringSanitizer.SanitizeMongoDbConnectionString(
            connectionString);

        // Assert
        result.Should().Be("***");
    }
}