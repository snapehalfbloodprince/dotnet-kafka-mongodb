using System.Net;
using System.Text.Json;
using FluentAssertions;
using KafkaRouter.Worker.IntegrationTests.Infrastructure;

namespace KafkaRouter.Worker.IntegrationTests.Metrics;

public sealed class MetricsEndpointTests
{
    [Fact]
    public async Task GetMetrics_ShouldReturnOkAndMetricsSnapshot()
    {
        // Arrange
        await using var factory = new KafkaRouterWebApplicationFactory();

        var client = factory.CreateClient();

        // Act
        var response = await client.GetAsync("/metrics");

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var content = await response.Content.ReadAsStringAsync();

        using var jsonDocument = JsonDocument.Parse(content);

        var root = jsonDocument.RootElement;

        root.GetProperty("instanceName").GetString().Should().NotBeNullOrWhiteSpace();
        root.GetProperty("processedMessages").GetInt64().Should().Be(0);
        root.GetProperty("deadLetterMessages").GetInt64().Should().Be(0);
        root.GetProperty("duplicateMessages").GetInt64().Should().Be(0);
        root.GetProperty("technicalFailures").GetInt64().Should().Be(0);

        root.GetProperty("totalProcessingDurationMs").GetInt64().Should().Be(0);
        root.GetProperty("averageProcessingDurationMs").ValueKind.Should().Be(JsonValueKind.Null);
        root.GetProperty("maxProcessingDurationMs").ValueKind.Should().Be(JsonValueKind.Null);
    }
}