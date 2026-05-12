using KafkaRouter.Worker.Health;
using KafkaRouter.Worker.Metrics;
using KafkaRouter.Worker.MongoDb.Repositories;
using KafkaRouter.Worker.Options;
using Microsoft.Extensions.Options;

namespace KafkaRouter.Worker.Diagnostics;

public static class DiagnosticEndpointsExtensions
{
    public static IEndpointRouteBuilder MapDiagnosticEndpoints(
        this IEndpointRouteBuilder endpoints)
    {
        var diagnosticsGroup = endpoints.MapGroup("/diagnostics");

        diagnosticsGroup.MapGet("/config", (
            IOptions<ApplicationOptions> applicationOptions,
            IOptions<WorkerOptions> workerOptions,
            IOptions<KafkaOptions> kafkaOptions,
            IOptions<MongoDbOptions> mongoDbOptions) =>
        {
            var response = new DiagnosticConfigResponse
            {
                ApplicationName = applicationOptions.Value.Name,
                ApplicationEnvironment = applicationOptions.Value.Environment,
                InstanceName = workerOptions.Value.InstanceName,
                CheckedAt = DateTimeOffset.UtcNow,
                Kafka = new KafkaDiagnosticConfig
                {
                    BootstrapServers = kafkaOptions.Value.BootstrapServers,
                    InputTopic = kafkaOptions.Value.InputTopic,
                    DeadLetterTopic = kafkaOptions.Value.DeadLetterTopic,
                    ConsumerGroupId = kafkaOptions.Value.ConsumerGroupId,
                    AutoOffsetReset = kafkaOptions.Value.AutoOffsetReset
                },
                MongoDb = new MongoDbDiagnosticConfig
                {
                    ConnectionString = ConnectionStringSanitizer.SanitizeMongoDbConnectionString(
                        mongoDbOptions.Value.ConnectionString),
                    DatabaseName = mongoDbOptions.Value.DatabaseName,
                    RoutingRulesCollectionName = mongoDbOptions.Value.RoutingRulesCollectionName,
                    ProcessedMessagesCollectionName = mongoDbOptions.Value.ProcessedMessagesCollectionName
                },
                Worker = new WorkerDiagnosticConfig
                {
                    ErrorDelayInSeconds = workerOptions.Value.ErrorDelayInSeconds,
                    ConsecutiveFailuresWarningThreshold = workerOptions.Value.ConsecutiveFailuresWarningThreshold,
                    TechnicalRetryMaxAttempts = workerOptions.Value.TechnicalRetryMaxAttempts,
                    TechnicalRetryInitialDelayInSeconds = workerOptions.Value.TechnicalRetryInitialDelayInSeconds,
                    TechnicalRetryMaxDelayInSeconds = workerOptions.Value.TechnicalRetryMaxDelayInSeconds,
                    ShutdownTimeoutInSeconds = workerOptions.Value.ShutdownTimeoutInSeconds
                }
            };

            return Results.Ok(response);
        });

        diagnosticsGroup.MapGet("/routing-rules", async (
            IRoutingRuleRepository routingRuleRepository,
            CancellationToken cancellationToken) =>
        {
            var rules = await routingRuleRepository.GetEnabledRulesAsync(
                cancellationToken);

            var response = new DiagnosticRoutingRulesResponse
            {
                CheckedAt = DateTimeOffset.UtcNow,
                Count = rules.Count,
                Rules = rules
                    .Select(rule => new DiagnosticRoutingRuleResponse
                    {
                        EventType = rule.EventType,
                        DestinationTopics = rule.DestinationTopics,
                        IsEnabled = rule.IsEnabled,
                        CreatedAt = rule.CreatedAt,
                        UpdatedAt = rule.UpdatedAt
                    })
                    .ToArray()
            };

            return Results.Ok(response);
        });

        diagnosticsGroup.MapGet("/status", async (
            IOptions<ApplicationOptions> applicationOptions,
            IOptions<WorkerOptions> workerOptions,
            IKafkaHealthCheckService kafkaHealthCheckService,
            IMongoDbHealthCheckService mongoDbHealthCheckService,
            IWorkerMetrics workerMetrics,
            CancellationToken cancellationToken) =>
        {
            var kafkaHealthy = await kafkaHealthCheckService.IsHealthyAsync(
                cancellationToken);

            var mongoDbHealthy = await mongoDbHealthCheckService.IsHealthyAsync(
                cancellationToken);

            var response = new DiagnosticStatusResponse
            {
                ApplicationName = applicationOptions.Value.Name,
                ApplicationEnvironment = applicationOptions.Value.Environment,
                InstanceName = workerOptions.Value.InstanceName,
                CheckedAt = DateTimeOffset.UtcNow,
                KafkaStatus = kafkaHealthy ? "Healthy" : "Unhealthy",
                MongoDbStatus = mongoDbHealthy ? "Healthy" : "Unhealthy",
                Metrics = workerMetrics.GetSnapshot()
            };

            return Results.Ok(response);
        });

        return endpoints;
    }
}