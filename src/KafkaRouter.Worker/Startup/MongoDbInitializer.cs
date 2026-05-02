using KafkaRouter.Worker.MongoDb.Repositories;

namespace KafkaRouter.Worker.Startup;

public sealed class MongoDbInitializer : IMongoDbInitializer
{
    private readonly ILogger<MongoDbInitializer> _logger;
    private readonly IRoutingRuleRepository _routingRuleRepository;

    public MongoDbInitializer(
        ILogger<MongoDbInitializer> logger,
        IRoutingRuleRepository routingRuleRepository)
    {
        _logger = logger;
        _routingRuleRepository = routingRuleRepository;
    }

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Inizializzazione MongoDB in corso.");

        await _routingRuleRepository.EnsureIndexesAsync(cancellationToken);
        await _routingRuleRepository.SeedDefaultRulesAsync(cancellationToken);

        var enabledRules = await _routingRuleRepository.GetEnabledRulesAsync(cancellationToken);

        _logger.LogInformation(
            "Routing rules abilitate presenti su MongoDB: {Count}.",
            enabledRules.Count);

        foreach (var rule in enabledRules)
        {
            _logger.LogInformation(
                "Routing rule MongoDB. EventType: {EventType}. DestinationTopics: {DestinationTopics}. IsEnabled: {IsEnabled}.",
                rule.EventType,
                string.Join(", ", rule.DestinationTopics),
                rule.IsEnabled);
        }

        _logger.LogInformation("Inizializzazione MongoDB completata.");
    }
}