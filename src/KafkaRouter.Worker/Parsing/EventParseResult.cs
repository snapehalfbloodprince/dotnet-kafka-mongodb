using KafkaRouter.Worker.Models;

namespace KafkaRouter.Worker.Parsing;

public sealed class EventParseResult
{
    private EventParseResult(
        bool isSuccess,
        EventEnvelope? eventEnvelope,
        string? errorCode,
        string? errorMessage)
    {
        IsSuccess = isSuccess;
        EventEnvelope = eventEnvelope;
        ErrorCode = errorCode;
        ErrorMessage = errorMessage;
    }

    public bool IsSuccess { get; }

    public EventEnvelope? EventEnvelope { get; }

    public string? ErrorCode { get; }

    public string? ErrorMessage { get; }

    public static EventParseResult Success(EventEnvelope eventEnvelope)
    {
        return new EventParseResult(
            isSuccess: true,
            eventEnvelope: eventEnvelope,
            errorCode: null,
            errorMessage: null);
    }

    public static EventParseResult Failure(
        string errorCode,
        string errorMessage)
    {
        return new EventParseResult(
            isSuccess: false,
            eventEnvelope: null,
            errorCode: errorCode,
            errorMessage: errorMessage);
    }
}