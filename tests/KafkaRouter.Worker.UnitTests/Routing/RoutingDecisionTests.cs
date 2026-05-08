using FluentAssertions;
using KafkaRouter.Worker.Routing;

namespace KafkaRouter.Worker.UnitTests.Routing;

public sealed class RoutingDecisionTests
{
    [Fact]
    public void RouteTo_WhenTopicsAreValid_ShouldReturnRoutableDecision()
    {
        // Act
        var decision = RoutingDecision.RouteTo(
            "events.crm",
            "events.notifications");

        // Assert
        decision.IsRoutable.Should().BeTrue();
        decision.DestinationTopics.Should().BeEquivalentTo(
            new[]
            {
                "events.crm",
                "events.notifications"
            });

        decision.ErrorCode.Should().BeNull();
        decision.ErrorMessage.Should().BeNull();
    }

    [Fact]
    public void RouteTo_WhenTopicsContainNullEmptyAndDuplicates_ShouldNormalizeTopics()
    {
        // Act
        var decision = RoutingDecision.RouteTo(
            new[]
            {
            "events.crm",
            "",
            " ",
            "events.crm",
            " EVENTS.CRM ",
            "events.notifications"
            });

        // Assert
        decision.IsRoutable.Should().BeTrue();
        decision.DestinationTopics.Should().BeEquivalentTo(
            new[]
            {
            "events.crm",
            "events.notifications"
            });
    }

    [Fact]
    public void DeadLetter_ShouldReturnNotRoutableDecision()
    {
        // Act
        var decision = RoutingDecision.DeadLetter(
            "UNKNOWN_EVENT_TYPE",
            "Event type not configured.");

        // Assert
        decision.IsRoutable.Should().BeFalse();
        decision.DestinationTopics.Should().BeEmpty();
        decision.ErrorCode.Should().Be("UNKNOWN_EVENT_TYPE");
        decision.ErrorMessage.Should().Be("Event type not configured.");
    }
}