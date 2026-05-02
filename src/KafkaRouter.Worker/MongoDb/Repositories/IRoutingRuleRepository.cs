using KafkaRouter.Worker.MongoDb.Documents;

namespace KafkaRouter.Worker.MongoDb.Repositories;

public interface IRoutingRuleRepository
{
    Task EnsureIndexesAsync(CancellationToken cancellationToken);

    Task SeedDefaultRulesAsync(CancellationToken cancellationToken);

    Task<IReadOnlyCollection<RoutingRuleDocument>> GetEnabledRulesAsync(CancellationToken cancellationToken);

    Task<RoutingRuleDocument?> GetEnabledRuleByEventTypeAsync(
        string eventType,
        CancellationToken cancellationToken);
}