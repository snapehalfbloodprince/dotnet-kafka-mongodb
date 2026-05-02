using KafkaRouter.Worker.Models;
using KafkaRouter.Worker.MongoDb.Repositories;

namespace KafkaRouter.Worker.Routing;

public sealed class MongoDbEventRoutingService : IEventRoutingService
{
    private readonly ILogger<MongoDbEventRoutingService> _logger;
    private readonly IRoutingRuleRepository _routingRuleRepository;

    public MongoDbEventRoutingService(
        ILogger<MongoDbEventRoutingService> logger,
        IRoutingRuleRepository routingRuleRepository)
    {
        _logger = logger;
        _routingRuleRepository = routingRuleRepository;
    }

    public async Task<RoutingDecision> GetRoutingDecisionAsync(
        EventEnvelope eventEnvelope,
        CancellationToken cancellationToken)
    {
        var eventType = eventEnvelope.EventType?.Trim();

        if (string.IsNullOrWhiteSpace(eventType))
        {
            return RoutingDecision.DeadLetter(
                errorCode: "MISSING_EVENT_TYPE",
                errorMessage: "Impossibile instradare l'evento perché eventType è vuoto.");
        }

        var routingRule = await _routingRuleRepository.GetEnabledRuleByEventTypeAsync(
            eventType,
            cancellationToken);

        if (routingRule is null)
        {
            return RoutingDecision.DeadLetter(
                errorCode: "ROUTING_RULE_NOT_FOUND",
                errorMessage: $"Nessuna routing rule abilitata trovata su MongoDB per eventType '{eventType}'.");
        }

        var destinationTopics = routingRule.DestinationTopics
            .Where(topic => !string.IsNullOrWhiteSpace(topic))
            .Select(topic => topic.Trim())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        if (destinationTopics.Length == 0)
        {
            return RoutingDecision.DeadLetter(
                errorCode: "EMPTY_DESTINATION_TOPICS",
                errorMessage: $"La routing rule MongoDB per eventType '{eventType}' non contiene topic di destinazione validi.");
        }

        _logger.LogInformation(
            "Routing rule caricata da MongoDB. EventType: {EventType}. DestinationTopics: {DestinationTopics}.",
            eventType,
            string.Join(", ", destinationTopics));

        return RoutingDecision.RouteTo(destinationTopics);
    }
}