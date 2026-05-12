namespace KafkaRouter.Worker.Diagnostics;

public sealed class DiagnosticRoutingRulesResponse
{
    public DateTimeOffset CheckedAt { get; init; }

    public int Count { get; init; }

    public IReadOnlyCollection<DiagnosticRoutingRuleResponse> Rules { get; init; }
        = Array.Empty<DiagnosticRoutingRuleResponse>();
}

public sealed class DiagnosticRoutingRuleResponse
{
    public string EventType { get; init; } = string.Empty;

    public IReadOnlyCollection<string> DestinationTopics { get; init; }
        = Array.Empty<string>();

    public bool IsEnabled { get; init; }

    public DateTimeOffset CreatedAt { get; init; }

    public DateTimeOffset? UpdatedAt { get; init; }
}