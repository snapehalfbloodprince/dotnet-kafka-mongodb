namespace KafkaRouter.Worker.Processing;

public static class ProcessingLogScopeExtensions
{
    public static IDisposable? BeginProcessingScope<T>(
        this ILogger<T> logger,
        ProcessingContext context)
    {
        return logger.BeginScope(new Dictionary<string, object?>
        {
            ["SourceTopic"] = context.SourceTopic,
            ["SourcePartition"] = context.SourcePartition,
            ["SourceOffset"] = context.SourceOffset,
            ["MessageKey"] = context.MessageKey,
            ["EventId"] = context.EventId,
            ["EventType"] = context.EventType,
            ["CorrelationId"] = context.GetEffectiveCorrelationId()
        });
    }
}