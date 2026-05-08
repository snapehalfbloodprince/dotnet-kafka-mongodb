using KafkaRouter.Worker;
using KafkaRouter.Worker.DeadLetter;
using KafkaRouter.Worker.Health;
using KafkaRouter.Worker.Kafka;
using KafkaRouter.Worker.MongoDb.Repositories;
using KafkaRouter.Worker.Options;
using KafkaRouter.Worker.Parsing;
using KafkaRouter.Worker.Routing;
using KafkaRouter.Worker.Startup;
using Microsoft.Extensions.Options;
using KafkaRouter.Worker.Metrics;
using KafkaRouter.Worker.Processing;
using KafkaRouter.Worker.DependencyInjection;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddValidatedOptions(builder.Configuration);

builder.Services.AddSingleton<IKafkaMessageConsumer, KafkaMessageConsumer>();
builder.Services.AddSingleton<IKafkaMessageProducer, KafkaMessageProducer>();

builder.Services.AddSingleton<IEventEnvelopeParser, EventEnvelopeParser>();
builder.Services.AddSingleton<IEventRoutingService, MongoDbEventRoutingService>();
builder.Services.AddSingleton<IDeadLetterMessageFactory, DeadLetterMessageFactory>();
builder.Services.AddSingleton<IMessageProcessingService, MessageProcessingService>();

builder.Services.AddSingleton<IRoutingRuleRepository, RoutingRuleRepository>();
builder.Services.AddSingleton<IProcessedMessageRepository, ProcessedMessageRepository>();
builder.Services.AddSingleton<IMongoDbInitializer, MongoDbInitializer>();

builder.Services.AddSingleton<IKafkaHealthCheckService, KafkaHealthCheckService>();
builder.Services.AddSingleton<IMongoDbHealthCheckService, MongoDbHealthCheckService>();

builder.Services.AddSingleton<IWorkerMetrics, InMemoryWorkerMetrics>();

builder.Services.AddHostedService<Worker>();

var app = builder.Build();

await InitializeMongoDbAsync(app.Services);

app.MapGet("/health/live", (
    IOptions<WorkerOptions> workerOptions) =>
{
    var response = new HealthResponse
    {
        Status = "Healthy",
        InstanceName = workerOptions.Value.InstanceName,
        CheckedAt = DateTimeOffset.UtcNow,
        Checks = new Dictionary<string, string>
        {
            ["process"] = "Healthy"
        }
    };

    return Results.Ok(response);
});

app.MapGet("/health/ready", async (
    IKafkaHealthCheckService kafkaHealthCheckService,
    IMongoDbHealthCheckService mongoDbHealthCheckService,
    IOptions<WorkerOptions> workerOptions,
    CancellationToken cancellationToken) =>
{
    var kafkaHealthy = await kafkaHealthCheckService.IsHealthyAsync(cancellationToken);
    var mongoDbHealthy = await mongoDbHealthCheckService.IsHealthyAsync(cancellationToken);

    var allHealthy = kafkaHealthy && mongoDbHealthy;

    var response = new HealthResponse
    {
        Status = allHealthy ? "Healthy" : "Unhealthy",
        InstanceName = workerOptions.Value.InstanceName,
        CheckedAt = DateTimeOffset.UtcNow,
        Checks = new Dictionary<string, string>
        {
            ["kafka"] = kafkaHealthy ? "Healthy" : "Unhealthy",
            ["mongodb"] = mongoDbHealthy ? "Healthy" : "Unhealthy"
        }
    };

    return allHealthy
        ? Results.Ok(response)
        : Results.Json(
            response,
            statusCode: StatusCodes.Status503ServiceUnavailable);
});

app.MapGet("/metrics", (
    IWorkerMetrics workerMetrics) =>
{
    var snapshot = workerMetrics.GetSnapshot();

    return Results.Ok(snapshot);
});

await app.RunAsync();

static async Task InitializeMongoDbAsync(IServiceProvider serviceProvider)
{
    using var scope = serviceProvider.CreateScope();

    var mongoDbInitializer = scope.ServiceProvider.GetRequiredService<IMongoDbInitializer>();
    var logger = scope.ServiceProvider.GetRequiredService<ILoggerFactory>()
        .CreateLogger("MongoDbStartup");

    try
    {
        await mongoDbInitializer.InitializeAsync(CancellationToken.None);
    }
    catch (Exception exception)
    {
        logger.LogCritical(
            exception,
            "Errore critico durante l'inizializzazione MongoDB. L'applicazione verrà terminata.");

        throw;
    }
}