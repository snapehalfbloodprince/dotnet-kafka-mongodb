namespace KafkaRouter.Worker.Routing;

public sealed class RoutingDecision
{
    private RoutingDecision(
        bool isRoutable,
        IReadOnlyCollection<string> destinationTopics,
        string? errorCode,
        string? errorMessage)
    {
        IsRoutable = isRoutable;
        DestinationTopics = destinationTopics;
        ErrorCode = errorCode;
        ErrorMessage = errorMessage;
    }

    public bool IsRoutable { get; }

    public IReadOnlyCollection<string> DestinationTopics { get; }

    public string? ErrorCode { get; }

    public string? ErrorMessage { get; }

    public static RoutingDecision RouteTo(params string[] destinationTopics)
    {
        return RouteTo((IEnumerable<string>)destinationTopics);
    }

    public static RoutingDecision RouteTo(IEnumerable<string> destinationTopics)
    {
        var normalizedDestinationTopics = destinationTopics
            .Where(topic => !string.IsNullOrWhiteSpace(topic))
            .Select(topic => topic.Trim())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        return new RoutingDecision(
            isRoutable: true,
            destinationTopics: normalizedDestinationTopics,
            errorCode: null,
            errorMessage: null);
    }

    public static RoutingDecision DeadLetter(
        string errorCode,
        string errorMessage)
    {
        return new RoutingDecision(
            isRoutable: false,
            destinationTopics: Array.Empty<string>(),
            errorCode: errorCode,
            errorMessage: errorMessage);
    }
}