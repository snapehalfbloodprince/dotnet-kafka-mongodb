using Confluent.Kafka;
using FluentAssertions;
using KafkaRouter.Worker.Metrics;
using KafkaRouter.Worker.Options;
using KafkaRouter.Worker.Processing;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace KafkaRouter.Worker.UnitTests.Processing;

public sealed class MessageProcessingRetryServiceTests
{
    private readonly Mock<IMessageProcessingService> _messageProcessingServiceMock = new();
    private readonly Mock<IWorkerMetrics> _workerMetricsMock = new();

    [Fact]
    public async Task ProcessWithRetryAsync_WhenProcessingSucceeds_ShouldCallProcessingServiceOnceAndReturnResult()
    {
        // Arrange
        var consumeResult = CreateConsumeResult();

        var expectedResult = MessageProcessingResult.ProcessedSuccessfully(
            eventId: "event-001",
            eventType: "CustomerCreated",
            correlationId: "correlation-001",
            processingDurationMs: 12);

        _messageProcessingServiceMock
            .Setup(service => service.ProcessAsync(
                consumeResult,
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedResult);

        var sut = CreateSut(
            technicalRetryMaxAttempts: 3,
            technicalRetryInitialDelayInSeconds: 1,
            technicalRetryMaxDelayInSeconds: 2,
            errorDelayInSeconds: 1);

        // Act
        var result = await sut.ProcessWithRetryAsync(
            consumeResult,
            CancellationToken.None);

        // Assert
        result.Should().BeSameAs(expectedResult);

        _messageProcessingServiceMock.Verify(
            service => service.ProcessAsync(
                consumeResult,
                It.IsAny<CancellationToken>()),
            Times.Once);

        _workerMetricsMock.Verify(
            metrics => metrics.IncrementTechnicalFailures(It.IsAny<string>()),
            Times.Never);
    }

    [Fact]
    public async Task ProcessWithRetryAsync_WhenProcessingFailsOnceThenSucceeds_ShouldRetryAndReturnSuccess()
    {
        // Arrange
        var consumeResult = CreateConsumeResult();

        var expectedResult = MessageProcessingResult.ProcessedSuccessfully(
            eventId: "event-001",
            eventType: "CustomerCreated",
            correlationId: "correlation-001",
            processingDurationMs: 12);

        _messageProcessingServiceMock
            .SetupSequence(service => service.ProcessAsync(
                consumeResult,
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Temporary failure."))
            .ReturnsAsync(expectedResult);

        var sut = CreateSut(
            technicalRetryMaxAttempts: 3,
            technicalRetryInitialDelayInSeconds: 0,
            technicalRetryMaxDelayInSeconds: 0,
            errorDelayInSeconds: 1);

        // Act
        var result = await sut.ProcessWithRetryAsync(
            consumeResult,
            CancellationToken.None);

        // Assert
        result.Should().BeSameAs(expectedResult);

        _messageProcessingServiceMock.Verify(
            service => service.ProcessAsync(
                consumeResult,
                It.IsAny<CancellationToken>()),
            Times.Exactly(2));

        _workerMetricsMock.Verify(
            metrics => metrics.IncrementTechnicalFailures("MESSAGE_PROCESSING_TECHNICAL_ERROR"),
            Times.Once);
    }

    [Fact]
    public async Task ProcessWithRetryAsync_WhenProcessingKeepsFailingUntilCancellation_ShouldRetryAndIncrementTechnicalFailures()
    {
        // Arrange
        var consumeResult = CreateConsumeResult();

        _messageProcessingServiceMock
            .Setup(service => service.ProcessAsync(
                consumeResult,
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Persistent failure."));

        var sut = CreateSut(
            technicalRetryMaxAttempts: 2,
            technicalRetryInitialDelayInSeconds: 0,
            technicalRetryMaxDelayInSeconds: 0,
            errorDelayInSeconds: 0);

        using var cancellationTokenSource = new CancellationTokenSource();

        cancellationTokenSource.CancelAfter(TimeSpan.FromMilliseconds(50));

        // Act
        var act = async () => await sut.ProcessWithRetryAsync(
            consumeResult,
            cancellationTokenSource.Token);

        // Assert
        await act.Should().ThrowAsync<OperationCanceledException>();

        _messageProcessingServiceMock.Verify(
            service => service.ProcessAsync(
                consumeResult,
                It.IsAny<CancellationToken>()),
            Times.AtLeast(2));

        _workerMetricsMock.Verify(
            metrics => metrics.IncrementTechnicalFailures("MESSAGE_PROCESSING_TECHNICAL_ERROR"),
            Times.AtLeast(2));
    }

    private MessageProcessingRetryService CreateSut(
    int technicalRetryMaxAttempts,
    int technicalRetryInitialDelayInSeconds,
    int technicalRetryMaxDelayInSeconds,
    int errorDelayInSeconds)
    {
        var workerOptions = Microsoft.Extensions.Options.Options.Create(new WorkerOptions
        {
            InstanceName = "worker-test",
            ErrorDelayInSeconds = errorDelayInSeconds,
            ConsecutiveFailuresWarningThreshold = 5,
            TechnicalRetryMaxAttempts = technicalRetryMaxAttempts,
            TechnicalRetryInitialDelayInSeconds = technicalRetryInitialDelayInSeconds,
            TechnicalRetryMaxDelayInSeconds = technicalRetryMaxDelayInSeconds
        });

        return new MessageProcessingRetryService(
            NullLogger<MessageProcessingRetryService>.Instance,
            _messageProcessingServiceMock.Object,
            _workerMetricsMock.Object,
            workerOptions);
    }

    private static ConsumeResult<string, string> CreateConsumeResult()
    {
        return new ConsumeResult<string, string>
        {
            Topic = "events.inbound",
            Partition = new Partition(0),
            Offset = new Offset(10),
            Message = new Message<string, string>
            {
                Key = "event-001",
                Value = """
                {
                  "eventId": "event-001",
                  "eventType": "CustomerCreated"
                }
                """
            }
        };
    }
}