using FluentAssertions;
using KafkaRouter.Worker.Options;
using KafkaRouter.Worker.Options.Validation;

namespace KafkaRouter.Worker.UnitTests.Options;

public sealed class WorkerOptionsValidatorTests
{
    private readonly WorkerOptionsValidator _sut = new();

    [Fact]
    public void Validate_WhenOptionsAreValid_ShouldReturnSuccess()
    {
        // Arrange
        var options = new WorkerOptions
        {
            InstanceName = "worker-test",
            ErrorDelayInSeconds = 5,
            ConsecutiveFailuresWarningThreshold = 5,
            TechnicalRetryMaxAttempts = 3,
            TechnicalRetryInitialDelayInSeconds = 1,
            TechnicalRetryMaxDelayInSeconds = 10,
            ShutdownTimeoutInSeconds = 30
        };

        // Act
        var result = _sut.Validate(null, options);

        // Assert
        result.Succeeded.Should().BeTrue();
        result.Failures.Should().BeNullOrEmpty();
    }

    [Fact]
    public void Validate_WhenRetryMaxDelayIsLowerThanInitialDelay_ShouldReturnFailure()
    {
        // Arrange
        var options = new WorkerOptions
        {
            InstanceName = "worker-test",
            ErrorDelayInSeconds = 5,
            ConsecutiveFailuresWarningThreshold = 5,
            TechnicalRetryMaxAttempts = 3,
            TechnicalRetryInitialDelayInSeconds = 10,
            TechnicalRetryMaxDelayInSeconds = 1,
            ShutdownTimeoutInSeconds = 0
        };

        // Act
        var result = _sut.Validate(null, options);

        // Assert
        result.Failed.Should().BeTrue();
        result.Failures.Should().Contain(
            "Worker:TechnicalRetryMaxDelayInSeconds deve essere maggiore o uguale a Worker:TechnicalRetryInitialDelayInSeconds.");
        result.Failures.Should().Contain(error => error.Contains("ShutdownTimeoutInSeconds"));
    }

    [Fact]
    public void Validate_WhenNumericValuesAreInvalid_ShouldReturnFailures()
    {
        // Arrange
        var options = new WorkerOptions
        {
            InstanceName = "",
            ErrorDelayInSeconds = 0,
            ConsecutiveFailuresWarningThreshold = 0,
            TechnicalRetryMaxAttempts = 0,
            TechnicalRetryInitialDelayInSeconds = 0,
            TechnicalRetryMaxDelayInSeconds = 0,
            ShutdownTimeoutInSeconds = 0
        };

        // Act
        var result = _sut.Validate(null, options);

        // Assert
        result.Failed.Should().BeTrue();
        result.Failures.Should().NotBeNullOrEmpty();
        result.Failures.Should().Contain(error => error.Contains("InstanceName"));
        result.Failures.Should().Contain(error => error.Contains("ErrorDelayInSeconds"));
        result.Failures.Should().Contain(error => error.Contains("ConsecutiveFailuresWarningThreshold"));
        result.Failures.Should().Contain(error => error.Contains("TechnicalRetryMaxAttempts"));
        result.Failures.Should().Contain(error => error.Contains("ShutdownTimeoutInSeconds"));
    }
}