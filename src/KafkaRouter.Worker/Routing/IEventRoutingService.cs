using KafkaRouter.Worker.Models;

namespace KafkaRouter.Worker.Routing;

public interface IEventRoutingService
{
    Task<RoutingDecision> GetRoutingDecisionAsync(
        EventEnvelope eventEnvelope,
        CancellationToken cancellationToken);
}