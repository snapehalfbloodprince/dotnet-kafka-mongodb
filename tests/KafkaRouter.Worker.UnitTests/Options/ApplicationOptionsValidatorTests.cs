using FluentAssertions;
using KafkaRouter.Worker.Options;
using KafkaRouter.Worker.Options.Validation;

namespace KafkaRouter.Worker.UnitTests.Options;

public sealed class ApplicationOptionsValidatorTests
{
    private readonly ApplicationOptionsValidator _sut = new();

    [Fact]
    public void Validate_WhenOptionsAreValid_ShouldReturnSuccess()
    {
        // Arrange
        var options = new ApplicationOptions
        {
            Name = "KafkaRouter.Worker",
            Environment = "Testing"
        };

        // Act
        var result = _sut.Validate(null, options);

        // Assert
        result.Succeeded.Should().BeTrue();
    }

    [Fact]
    public void Validate_WhenNameIsMissing_ShouldReturnFailure()
    {
        // Arrange
        var options = new ApplicationOptions
        {
            Name = "",
            Environment = "Testing"
        };

        // Act
        var result = _sut.Validate(null, options);

        // Assert
        result.Failed.Should().BeTrue();
        result.Failures.Should().Contain(error => error.Contains("Application:Name"));
    }

    [Fact]
    public void Validate_WhenEnvironmentIsMissing_ShouldReturnFailure()
    {
        // Arrange
        var options = new ApplicationOptions
        {
            Name = "KafkaRouter.Worker",
            Environment = ""
        };

        // Act
        var result = _sut.Validate(null, options);

        // Assert
        result.Failed.Should().BeTrue();
        result.Failures.Should().Contain(error => error.Contains("Application:Environment"));
    }
}