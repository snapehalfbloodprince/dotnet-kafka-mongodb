using System.Text.Json;
using KafkaRouter.Worker.Models;

namespace KafkaRouter.Worker.Parsing;

public sealed class EventEnvelopeParser : IEventEnvelopeParser
{
    private static readonly JsonSerializerOptions JsonSerializerOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public EventParseResult Parse(string? rawMessage)
    {
        if (string.IsNullOrWhiteSpace(rawMessage))
        {
            return EventParseResult.Failure(
                errorCode: "EMPTY_MESSAGE",
                errorMessage: "Il messaggio Kafka è vuoto.");
        }

        EventEnvelope? eventEnvelope;

        try
        {
            eventEnvelope = JsonSerializer.Deserialize<EventEnvelope>(
                rawMessage,
                JsonSerializerOptions);
        }
        catch (JsonException exception)
        {
            return EventParseResult.Failure(
                errorCode: "INVALID_JSON",
                errorMessage: $"Il messaggio Kafka non è un JSON valido. Dettaglio: {exception.Message}");
        }

        if (eventEnvelope is null)
        {
            return EventParseResult.Failure(
                errorCode: "NULL_EVENT",
                errorMessage: "Il messaggio Kafka non contiene un evento valido.");
        }

        var validationError = Validate(eventEnvelope);

        if (validationError is not null)
        {
            return EventParseResult.Failure(
                errorCode: validationError.Value.ErrorCode,
                errorMessage: validationError.Value.ErrorMessage);
        }

        return EventParseResult.Success(eventEnvelope);
    }

    private static ValidationError? Validate(EventEnvelope eventEnvelope)
    {
        if (string.IsNullOrWhiteSpace(eventEnvelope.EventId))
        {
            return new ValidationError(
                ErrorCode: "MISSING_EVENT_ID",
                ErrorMessage: "Il campo eventId è obbligatorio.");
        }

        if (string.IsNullOrWhiteSpace(eventEnvelope.EventType))
        {
            return new ValidationError(
                ErrorCode: "MISSING_EVENT_TYPE",
                ErrorMessage: "Il campo eventType è obbligatorio.");
        }

        if (eventEnvelope.OccurredAt is null)
        {
            return new ValidationError(
                ErrorCode: "MISSING_OCCURRED_AT",
                ErrorMessage: "Il campo occurredAt è obbligatorio o non è una data valida.");
        }

        if (eventEnvelope.Payload.ValueKind is JsonValueKind.Undefined or JsonValueKind.Null)
        {
            return new ValidationError(
                ErrorCode: "MISSING_PAYLOAD",
                ErrorMessage: "Il campo payload è obbligatorio.");
        }

        return null;
    }

    private readonly record struct ValidationError(
        string ErrorCode,
        string ErrorMessage);
}