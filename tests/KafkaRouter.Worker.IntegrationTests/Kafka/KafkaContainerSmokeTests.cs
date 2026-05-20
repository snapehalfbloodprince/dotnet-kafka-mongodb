using FluentAssertions;
using KafkaRouter.Worker.IntegrationTests.Kafka.Infrastructure;

namespace KafkaRouter.Worker.IntegrationTests.Kafka;

public sealed class KafkaContainerSmokeTests
    : IClassFixture<KafkaTestcontainerFixture>
{
    private readonly KafkaTestcontainerFixture _fixture;

    public KafkaContainerSmokeTests(
        KafkaTestcontainerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task KafkaContainer_ShouldProduceAndConsumeMessage()
    {
        // Arrange
        var topic = $"smoke-topic-{Guid.NewGuid():N}";
        var groupId = $"smoke-group-{Guid.NewGuid():N}";
        var expectedValue = "Hello Kafka from Testcontainers";

        await KafkaTestClient.CreateTopicAsync(
            _fixture.BootstrapServers,
            topic);

        // Act
        await KafkaTestClient.ProduceAsync(
            _fixture.BootstrapServers,
            topic,
            key: "key-001",
            value: expectedValue);

        var actualValue = KafkaTestClient.ConsumeSingleValue(
            _fixture.BootstrapServers,
            topic,
            groupId,
            TimeSpan.FromSeconds(15));

        // Assert
        actualValue.Should().Be(expectedValue);
    }
}