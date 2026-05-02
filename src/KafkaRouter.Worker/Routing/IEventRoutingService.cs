using KafkaRouter.Worker.Models;

namespace KafkaRouter.Worker.Routing;

public interface IEventRoutingService
{
    RoutingDecision GetRoutingDecision(EventEnvelope eventEnvelope);
}