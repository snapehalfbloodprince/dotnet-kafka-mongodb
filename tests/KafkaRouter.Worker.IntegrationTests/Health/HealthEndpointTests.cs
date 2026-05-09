using System.Net;
using FluentAssertions;
using KafkaRouter.Worker.IntegrationTests.Infrastructure;
using System.Text.Json;

namespace KafkaRouter.Worker.IntegrationTests.Health;

public sealed class HealthEndpointTests
{
    [Fact]
    public async Task GetHealthLive_ShouldReturnOk()
    {
        // Arrange
        await using var factory = new KafkaRouterWebApplicationFactory();

        var client = factory.CreateClient();

        // Act
        var response = await client.GetAsync("/health/live");

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var content = await response.Content.ReadAsStringAsync();

        using var jsonDocument = JsonDocument.Parse(content);

        var root = jsonDocument.RootElement;

        root.GetProperty("status").GetString().Should().Be("Healthy");
        root.GetProperty("instanceName").GetString().Should().NotBeNullOrWhiteSpace();
        root.GetProperty("checks").GetProperty("process").GetString().Should().Be("Healthy");
    }

    [Fact]
    public async Task GetHealthReady_WhenKafkaAndMongoDbAreHealthy_ShouldReturnOk()
    {
        // Arrange
        await using var factory = new KafkaRouterWebApplicationFactory(
            isKafkaHealthy: true,
            isMongoDbHealthy: true);

        var client = factory.CreateClient();

        // Act
        var response = await client.GetAsync("/health/ready");

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var content = await response.Content.ReadAsStringAsync();

        content.Should().Contain("Healthy");
        content.Should().Contain("kafka");
        content.Should().Contain("mongodb");
    }

    [Fact]
    public async Task GetHealthReady_WhenKafkaIsUnhealthy_ShouldReturnServiceUnavailable()
    {
        // Arrange
        await using var factory = new KafkaRouterWebApplicationFactory(
            isKafkaHealthy: false,
            isMongoDbHealthy: true);

        var client = factory.CreateClient();

        // Act
        var response = await client.GetAsync("/health/ready");

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);

        var content = await response.Content.ReadAsStringAsync();

        content.Should().Contain("Unhealthy");
        content.Should().Contain("kafka");
    }

    [Fact]
    public async Task GetHealthReady_WhenMongoDbIsUnhealthy_ShouldReturnServiceUnavailable()
    {
        // Arrange
        await using var factory = new KafkaRouterWebApplicationFactory(
            isKafkaHealthy: true,
            isMongoDbHealthy: false);

        var client = factory.CreateClient();

        // Act
        var response = await client.GetAsync("/health/ready");

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);

        var content = await response.Content.ReadAsStringAsync();

        content.Should().Contain("Unhealthy");
        content.Should().Contain("mongodb");
    }
}