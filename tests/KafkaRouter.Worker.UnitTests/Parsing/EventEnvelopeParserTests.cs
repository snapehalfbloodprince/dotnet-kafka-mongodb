using FluentAssertions;
using KafkaRouter.Worker.Parsing;

namespace KafkaRouter.Worker.UnitTests.Parsing;

public sealed class EventEnvelopeParserTests
{
    private readonly EventEnvelopeParser _sut = new();

    [Fact]
    public void Parse_WhenMessageIsValid_ShouldReturnSuccess()
    {
        // Arrange
        const string rawMessage = """
        {
          "eventId": "event-001",
          "eventType": "CustomerCreated",
          "occurredAt": "2026-05-08T10:00:00Z",
          "source": "unit-test",
          "correlationId": "correlation-001",
          "payload": {
            "customerId": "CUST-001"
          }
        }
        """;

        // Act
        var result = _sut.Parse(rawMessage);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.EventEnvelope.Should().NotBeNull();

        result.EventEnvelope!.EventId.Should().Be("event-001");
        result.EventEnvelope.EventType.Should().Be("CustomerCreated");
        result.EventEnvelope.Source.Should().Be("unit-test");
        result.EventEnvelope.CorrelationId.Should().Be("correlation-001");
        result.EventEnvelope.Payload.ValueKind.Should().Be(System.Text.Json.JsonValueKind.Object);

        result.ErrorCode.Should().BeNull();
        result.ErrorMessage.Should().BeNull();
    }

    [Fact]
    public void Parse_WhenMessageIsNull_ShouldReturnEmptyMessageFailure()
    {
        // Act
        var result = _sut.Parse(null);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.EventEnvelope.Should().BeNull();
        result.ErrorCode.Should().Be("EMPTY_MESSAGE");
        result.ErrorMessage.Should().Contain("vuoto");
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Parse_WhenMessageIsEmptyOrWhiteSpace_ShouldReturnEmptyMessageFailure(string rawMessage)
    {
        // Act
        var result = _sut.Parse(rawMessage);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.EventEnvelope.Should().BeNull();
        result.ErrorCode.Should().Be("EMPTY_MESSAGE");
    }

    [Fact]
    public void Parse_WhenMessageIsInvalidJson_ShouldReturnInvalidJsonFailure()
    {
        // Arrange
        const string rawMessage = "questo non è json";

        // Act
        var result = _sut.Parse(rawMessage);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.EventEnvelope.Should().BeNull();
        result.ErrorCode.Should().Be("INVALID_JSON");
        result.ErrorMessage.Should().Contain("JSON valido");
    }

    [Fact]
    public void Parse_WhenEventIdIsMissing_ShouldReturnMissingEventIdFailure()
    {
        // Arrange
        const string rawMessage = """
        {
          "eventType": "CustomerCreated",
          "occurredAt": "2026-05-08T10:00:00Z",
          "payload": {
            "customerId": "CUST-001"
          }
        }
        """;

        // Act
        var result = _sut.Parse(rawMessage);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.ErrorCode.Should().Be("MISSING_EVENT_ID");
    }

    [Fact]
    public void Parse_WhenEventTypeIsMissing_ShouldReturnMissingEventTypeFailure()
    {
        // Arrange
        const string rawMessage = """
        {
          "eventId": "event-001",
          "occurredAt": "2026-05-08T10:00:00Z",
          "payload": {
            "customerId": "CUST-001"
          }
        }
        """;

        // Act
        var result = _sut.Parse(rawMessage);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.ErrorCode.Should().Be("MISSING_EVENT_TYPE");
    }

    [Fact]
    public void Parse_WhenOccurredAtIsMissing_ShouldReturnMissingOccurredAtFailure()
    {
        // Arrange
        const string rawMessage = """
        {
          "eventId": "event-001",
          "eventType": "CustomerCreated",
          "payload": {
            "customerId": "CUST-001"
          }
        }
        """;

        // Act
        var result = _sut.Parse(rawMessage);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.ErrorCode.Should().Be("MISSING_OCCURRED_AT");
    }

    [Fact]
    public void Parse_WhenPayloadIsMissing_ShouldReturnMissingPayloadFailure()
    {
        // Arrange
        const string rawMessage = """
        {
          "eventId": "event-001",
          "eventType": "CustomerCreated",
          "occurredAt": "2026-05-08T10:00:00Z"
        }
        """;

        // Act
        var result = _sut.Parse(rawMessage);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.ErrorCode.Should().Be("MISSING_PAYLOAD");
    }

    [Fact]
    public void Parse_WhenPropertyNamesHaveDifferentCasing_ShouldReturnSuccess()
    {
        // Arrange
        const string rawMessage = """
        {
          "EventId": "event-001",
          "EventType": "CustomerCreated",
          "OccurredAt": "2026-05-08T10:00:00Z",
          "Source": "unit-test",
          "CorrelationId": "correlation-001",
          "Payload": {
            "customerId": "CUST-001"
          }
        }
        """;

        // Act
        var result = _sut.Parse(rawMessage);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.EventEnvelope!.EventId.Should().Be("event-001");
        result.EventEnvelope.EventType.Should().Be("CustomerCreated");
    }
}