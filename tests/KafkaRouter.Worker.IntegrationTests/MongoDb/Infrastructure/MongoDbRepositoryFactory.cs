using KafkaRouter.Worker.MongoDb.Repositories;
using KafkaRouter.Worker.Options;
using Microsoft.Extensions.Logging.Abstractions;

namespace KafkaRouter.Worker.IntegrationTests.MongoDb.Infrastructure;

public static class MongoDbRepositoryFactory
{
    public static MongoDbOptions CreateMongoDbOptions(
        MongoDbTestcontainerFixture fixture)
    {
        return new MongoDbOptions
        {
            ConnectionString = fixture.ConnectionString,
            DatabaseName = fixture.DatabaseName,
            RoutingRulesCollectionName = "routing_rules",
            ProcessedMessagesCollectionName = "processed_messages"
        };
    }

    public static RoutingRuleRepository CreateRoutingRuleRepository(
        MongoDbTestcontainerFixture fixture)
    {
        var options = Microsoft.Extensions.Options.Options.Create(
            CreateMongoDbOptions(fixture));

        return new RoutingRuleRepository(
            options,
            NullLogger<RoutingRuleRepository>.Instance);
    }

    public static ProcessedMessageRepository CreateProcessedMessageRepository(
        MongoDbTestcontainerFixture fixture)
    {
        var options = Microsoft.Extensions.Options.Options.Create(
            CreateMongoDbOptions(fixture));

        return new ProcessedMessageRepository(
            options,
            NullLogger<ProcessedMessageRepository>.Instance);
    }
}