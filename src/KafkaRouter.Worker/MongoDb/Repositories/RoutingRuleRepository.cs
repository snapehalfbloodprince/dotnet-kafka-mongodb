using KafkaRouter.Worker.MongoDb.Documents;
using KafkaRouter.Worker.Options;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace KafkaRouter.Worker.MongoDb.Repositories;

public sealed class RoutingRuleRepository : IRoutingRuleRepository
{
    private readonly ILogger<RoutingRuleRepository> _logger;
    private readonly IMongoCollection<RoutingRuleDocument> _collection;

    public RoutingRuleRepository(
        IOptions<MongoDbOptions> options,
        ILogger<RoutingRuleRepository> logger)
    {
        _logger = logger;

        var mongoOptions = options.Value;

        var mongoClient = new MongoClient(mongoOptions.ConnectionString);
        var database = mongoClient.GetDatabase(mongoOptions.DatabaseName);

        _collection = database.GetCollection<RoutingRuleDocument>(
            mongoOptions.RoutingRulesCollectionName);
    }

    public async Task EnsureIndexesAsync(CancellationToken cancellationToken)
    {
        var indexKeys = Builders<RoutingRuleDocument>
            .IndexKeys
            .Ascending(rule => rule.EventType);

        var indexModel = new CreateIndexModel<RoutingRuleDocument>(
            indexKeys,
            new CreateIndexOptions
            {
                Name = "ux_routing_rules_event_type",
                Unique = true
            });

        await _collection.Indexes.CreateOneAsync(
            indexModel,
            cancellationToken: cancellationToken);

        _logger.LogInformation(
            "Indice MongoDB creato/verificato sulla collection routing_rules: {IndexName}.",
            "ux_routing_rules_event_type");
    }

    public async Task SeedDefaultRulesAsync(CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow;

        var defaultRules = new[]
        {
            new RoutingRuleDocument
            {
                EventType = "CustomerCreated",
                DestinationTopics = new[]
                {
                    "events.crm",
                    "events.notifications"
                },
                IsEnabled = true,
                CreatedAt = now,
                UpdatedAt = now
            },
            new RoutingRuleDocument
            {
                EventType = "InvoicePaid",
                DestinationTopics = new[]
                {
                    "events.billing",
                    "events.notifications"
                },
                IsEnabled = true,
                CreatedAt = now,
                UpdatedAt = now
            },
            new RoutingRuleDocument
            {
                EventType = "PaymentFailed",
                DestinationTopics = new[]
                {
                    "events.billing",
                    "events.notifications"
                },
                IsEnabled = true,
                CreatedAt = now,
                UpdatedAt = now
            }
        };

        foreach (var defaultRule in defaultRules)
        {
            var filter = Builders<RoutingRuleDocument>
                .Filter
                .Eq(rule => rule.EventType, defaultRule.EventType);

            var existingRule = await _collection
                .Find(filter)
                .FirstOrDefaultAsync(cancellationToken);

            if (existingRule is not null)
            {
                _logger.LogInformation(
                    "Routing rule già presente su MongoDB. EventType: {EventType}.",
                    defaultRule.EventType);

                continue;
            }

            await _collection.InsertOneAsync(
                defaultRule,
                cancellationToken: cancellationToken);

            _logger.LogInformation(
                "Routing rule inserita su MongoDB. EventType: {EventType}. DestinationTopics: {DestinationTopics}.",
                defaultRule.EventType,
                string.Join(", ", defaultRule.DestinationTopics));
        }
    }

    public async Task<IReadOnlyCollection<RoutingRuleDocument>> GetEnabledRulesAsync(
        CancellationToken cancellationToken)
    {
        var filter = Builders<RoutingRuleDocument>
            .Filter
            .Eq(rule => rule.IsEnabled, true);

        var rules = await _collection
            .Find(filter)
            .SortBy(rule => rule.EventType)
            .ToListAsync(cancellationToken);

        return rules;
    }

    public async Task<RoutingRuleDocument?> GetEnabledRuleByEventTypeAsync(
        string eventType,
        CancellationToken cancellationToken)
    {
        var filter = Builders<RoutingRuleDocument>.Filter.And(
            Builders<RoutingRuleDocument>.Filter.Eq(rule => rule.EventType, eventType),
            Builders<RoutingRuleDocument>.Filter.Eq(rule => rule.IsEnabled, true));

        var rule = await _collection
            .Find(filter)
            .FirstOrDefaultAsync(cancellationToken);

        return rule;
    }
}