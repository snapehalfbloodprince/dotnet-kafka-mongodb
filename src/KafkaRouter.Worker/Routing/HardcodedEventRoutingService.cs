using KafkaRouter.Worker.Models;

namespace KafkaRouter.Worker.Routing;

public sealed class HardcodedEventRoutingService : IEventRoutingService
{
    public RoutingDecision GetRoutingDecision(EventEnvelope eventEnvelope)
    {
        var eventType = eventEnvelope.EventType?.Trim();

        if (string.IsNullOrWhiteSpace(eventType))
        {
            return RoutingDecision.DeadLetter(
                errorCode: "MISSING_EVENT_TYPE",
                errorMessage: "Impossibile instradare l'evento perché eventType è vuoto.");
        }

        return eventType switch
        {
            "CustomerCreated" => RoutingDecision.RouteTo(
                "events.crm",
                "events.notifications"),

            "InvoicePaid" => RoutingDecision.RouteTo(
                "events.billing",
                "events.notifications"),

            "PaymentFailed" => RoutingDecision.RouteTo(
                "events.billing",
                "events.notifications"),

            _ => RoutingDecision.DeadLetter(
                errorCode: "UNKNOWN_EVENT_TYPE",
                errorMessage: $"Nessuna regola di routing configurata per eventType '{eventType}'.")
        };
    }
}