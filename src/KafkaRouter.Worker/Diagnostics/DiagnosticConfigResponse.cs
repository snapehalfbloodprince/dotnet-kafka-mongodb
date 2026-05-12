namespace KafkaRouter.Worker.Diagnostics;

public sealed class DiagnosticConfigResponse
{
    public string ApplicationName { get; init; } = string.Empty;

    public string ApplicationEnvironment { get; init; } = string.Empty;

    public string InstanceName { get; init; } = string.Empty;

    public DateTimeOffset CheckedAt { get; init; }

    public KafkaDiagnosticConfig Kafka { get; init; } = new();

    public MongoDbDiagnosticConfig MongoDb { get; init; } = new();

    public WorkerDiagnosticConfig Worker { get; init; } = new();
}

public sealed class KafkaDiagnosticConfig
{
    public string BootstrapServers { get; init; } = string.Empty;

    public string InputTopic { get; init; } = string.Empty;

    public string DeadLetterTopic { get; init; } = string.Empty;

    public string ConsumerGroupId { get; init; } = string.Empty;

    public string AutoOffsetReset { get; init; } = string.Empty;
}

public sealed class MongoDbDiagnosticConfig
{
    public string ConnectionString { get; init; } = string.Empty;

    public string DatabaseName { get; init; } = string.Empty;

    public string RoutingRulesCollectionName { get; init; } = string.Empty;

    public string ProcessedMessagesCollectionName { get; init; } = string.Empty;
}

public sealed class WorkerDiagnosticConfig
{
    public int ErrorDelayInSeconds { get; init; }

    public int ConsecutiveFailuresWarningThreshold { get; init; }

    public int TechnicalRetryMaxAttempts { get; init; }

    public int TechnicalRetryInitialDelayInSeconds { get; init; }

    public int TechnicalRetryMaxDelayInSeconds { get; init; }

    public int ShutdownTimeoutInSeconds { get; init; }
}