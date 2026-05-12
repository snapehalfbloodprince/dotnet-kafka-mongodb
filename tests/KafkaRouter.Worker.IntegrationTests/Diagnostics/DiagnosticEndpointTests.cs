using System.Net;
using System.Text.Json;
using FluentAssertions;
using KafkaRouter.Worker.IntegrationTests.Infrastructure;

namespace KafkaRouter.Worker.IntegrationTests.Diagnostics;

public sealed class DiagnosticEndpointTests
{
    [Fact]
    public async Task GetDiagnosticsConfig_ShouldReturnSanitizedConfiguration()
    {
        // Arrange
        await using var factory = new KafkaRouterWebApplicationFactory();

        var client = factory.CreateClient();

        // Act
        var response = await client.GetAsync("/diagnostics/config");

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var content = await response.Content.ReadAsStringAsync();

        using var jsonDocument = JsonDocument.Parse(content);

        var root = jsonDocument.RootElement;

        root.GetProperty("applicationName").GetString().Should().Be("KafkaRouter.Worker");
        root.GetProperty("applicationEnvironment").GetString().Should().Be("Testing");
        root.GetProperty("instanceName").GetString().Should().NotBeNullOrWhiteSpace();

        var kafka = root.GetProperty("kafka");

        kafka.GetProperty("inputTopic").GetString().Should().Be("events.inbound");
        kafka.GetProperty("deadLetterTopic").GetString().Should().Be("events.dead-letter");
        kafka.GetProperty("consumerGroupId").GetString().Should().Be("kafka-router-worker-local");

        var mongoDb = root.GetProperty("mongoDb");

        mongoDb.GetProperty("connectionString").GetString().Should().NotContain("rootpassword");
        mongoDb.GetProperty("connectionString").GetString().Should().Contain("***:***");
        mongoDb.GetProperty("databaseName").GetString().Should().Be("kafka_router");
    }

    [Fact]
    public async Task GetDiagnosticsRoutingRules_ShouldReturnEnabledRules()
    {
        // Arrange
        await using var factory = new KafkaRouterWebApplicationFactory();

        var client = factory.CreateClient();

        // Act
        var response = await client.GetAsync("/diagnostics/routing-rules");

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var content = await response.Content.ReadAsStringAsync();

        using var jsonDocument = JsonDocument.Parse(content);

        var root = jsonDocument.RootElement;

        root.GetProperty("count").GetInt32().Should().Be(2);

        var rules = root.GetProperty("rules");

        rules.GetArrayLength().Should().Be(2);

        rules[0].GetProperty("eventType").GetString().Should().NotBeNullOrWhiteSpace();
        rules[0].GetProperty("destinationTopics").GetArrayLength().Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task GetDiagnosticsStatus_ShouldReturnOperationalSummary()
    {
        // Arrange
        await using var factory = new KafkaRouterWebApplicationFactory();

        var client = factory.CreateClient();

        // Act
        var response = await client.GetAsync("/diagnostics/status");

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var content = await response.Content.ReadAsStringAsync();

        using var jsonDocument = JsonDocument.Parse(content);

        var root = jsonDocument.RootElement;

        root.GetProperty("applicationName").GetString().Should().Be("KafkaRouter.Worker");
        root.GetProperty("applicationEnvironment").GetString().Should().Be("Testing");
        root.GetProperty("instanceName").GetString().Should().NotBeNullOrWhiteSpace();

        root.GetProperty("kafkaStatus").GetString().Should().Be("Healthy");
        root.GetProperty("mongoDbStatus").GetString().Should().Be("Healthy");

        var metrics = root.GetProperty("metrics");

        metrics.GetProperty("processedMessages").GetInt64().Should().Be(0);
        metrics.GetProperty("deadLetterMessages").GetInt64().Should().Be(0);
        metrics.GetProperty("duplicateMessages").GetInt64().Should().Be(0);
        metrics.GetProperty("technicalFailures").GetInt64().Should().Be(0);
    }
}