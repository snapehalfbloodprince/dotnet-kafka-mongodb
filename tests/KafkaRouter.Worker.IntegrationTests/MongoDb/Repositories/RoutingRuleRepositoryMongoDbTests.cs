using FluentAssertions;
using KafkaRouter.Worker.IntegrationTests.MongoDb.Infrastructure;

namespace KafkaRouter.Worker.IntegrationTests.MongoDb.Repositories;

public sealed class RoutingRuleRepositoryMongoDbTests
    : IClassFixture<MongoDbTestcontainerFixture>
{
    private readonly MongoDbTestcontainerFixture _fixture;

    public RoutingRuleRepositoryMongoDbTests(
        MongoDbTestcontainerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task SeedDefaultRulesAsync_ShouldCreateDefaultRoutingRules()
    {
        // Arrange
        var repository = MongoDbRepositoryFactory.CreateRoutingRuleRepository(
            _fixture);

        await repository.EnsureIndexesAsync(CancellationToken.None);

        // Act
        await repository.SeedDefaultRulesAsync(CancellationToken.None);

        var rules = await repository.GetEnabledRulesAsync(CancellationToken.None);

        // Assert
        rules.Should().NotBeEmpty();

        rules.Should().Contain(rule =>
            rule.EventType == "CustomerCreated"
            && rule.IsEnabled
            && rule.DestinationTopics.Contains("events.crm"));

        rules.Should().Contain(rule =>
            rule.EventType == "InvoicePaid"
            && rule.IsEnabled
            && rule.DestinationTopics.Contains("events.billing"));

        rules.Should().Contain(rule =>
            rule.EventType == "PaymentFailed"
            && rule.IsEnabled
            && rule.DestinationTopics.Contains("events.notifications"));
    }

    [Fact]
    public async Task GetEnabledRuleByEventTypeAsync_WhenRuleExists_ShouldReturnRule()
    {
        // Arrange
        var repository = MongoDbRepositoryFactory.CreateRoutingRuleRepository(
            _fixture);

        await repository.EnsureIndexesAsync(CancellationToken.None);
        await repository.SeedDefaultRulesAsync(CancellationToken.None);

        // Act
        var rule = await repository.GetEnabledRuleByEventTypeAsync(
            "CustomerCreated",
            CancellationToken.None);

        // Assert
        rule.Should().NotBeNull();
        rule!.EventType.Should().Be("CustomerCreated");
        rule.IsEnabled.Should().BeTrue();
        rule.DestinationTopics.Should().Contain("events.crm");
    }

    [Fact]
    public async Task GetEnabledRuleByEventTypeAsync_WhenRuleDoesNotExist_ShouldReturnNull()
    {
        // Arrange
        var repository = MongoDbRepositoryFactory.CreateRoutingRuleRepository(
            _fixture);

        await repository.EnsureIndexesAsync(CancellationToken.None);
        await repository.SeedDefaultRulesAsync(CancellationToken.None);

        // Act
        var rule = await repository.GetEnabledRuleByEventTypeAsync(
            "UnknownEvent",
            CancellationToken.None);

        // Assert
        rule.Should().BeNull();
    }
}