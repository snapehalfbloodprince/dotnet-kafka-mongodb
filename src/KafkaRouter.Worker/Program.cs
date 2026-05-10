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

builder.Services.Configure<HostOptions>(options =>
{
    var workerOptions = builder.Configuration
        .GetSection(WorkerOptions.SectionName)
        .Get<WorkerOptions>() ?? new WorkerOptions();

    options.ShutdownTimeout = TimeSpan.FromSeconds(
        workerOptions.ShutdownTimeoutInSeconds);
});

builder.Services.AddSingleton<IKafkaMessageConsumer, KafkaMessageConsumer>();
builder.Services.AddSingleton<IKafkaMessageProducer, KafkaMessageProducer>();

builder.Services.AddSingleton<IEventEnvelopeParser, EventEnvelopeParser>();
builder.Services.AddSingleton<IEventRoutingService, MongoDbEventRoutingService>();
builder.Services.AddSingleton<IDeadLetterMessageFactory, DeadLetterMessageFactory>();
builder.Services.AddSingleton<IMessageProcessingService, MessageProcessingService>();
builder.Services.AddSingleton<IMessageProcessingRetryService, MessageProcessingRetryService>();

builder.Services.AddSingleton<IRoutingRuleRepository, RoutingRuleRepository>();
builder.Services.AddSingleton<IProcessedMessageRepository, ProcessedMessageRepository>();
builder.Services.AddSingleton<IMongoDbInitializer, MongoDbInitializer>();

builder.Services.AddSingleton<IKafkaHealthCheckService, KafkaHealthCheckService>();
builder.Services.AddSingleton<IMongoDbHealthCheckService, MongoDbHealthCheckService>();

builder.Services.AddSingleton<IWorkerMetrics, InMemoryWorkerMetrics>();

builder.Services.AddHostedService<Worker>();

var app = builder.Build();

if (!app.Environment.IsEnvironment("Testing"))
{
    await InitializeMongoDbAsync(app.Services);
}

app.MapGet("/health/live", (
    IOptions<ApplicationOptions> applicationOptions,
    IOptions<WorkerOptions> workerOptions) =>
{
    var response = new HealthResponse
    {
        Status = "Healthy",
        InstanceName = workerOptions.Value.InstanceName,
        CheckedAt = DateTimeOffset.UtcNow,
        Checks = new Dictionary<string, string>
        {
            ["application"] = applicationOptions.Value.Name,
            ["environment"] = applicationOptions.Value.Environment,
            ["process"] = "Healthy"
        }
    };

    return Results.Ok(response);
});

app.MapGet("/health/ready", async (
    IOptions<ApplicationOptions> applicationOptions,
    IOptions<WorkerOptions> workerOptions,
    IKafkaHealthCheckService kafkaHealthCheckService,
    IMongoDbHealthCheckService mongoDbHealthCheckService,
    CancellationToken cancellationToken) =>
{
    var kafkaHealthy = await kafkaHealthCheckService.IsHealthyAsync(cancellationToken);
    var mongoDbHealthy = await mongoDbHealthCheckService.IsHealthyAsync(cancellationToken);

    var isHealthy = kafkaHealthy && mongoDbHealthy;

    var response = new HealthResponse
    {
        Status = isHealthy ? "Healthy" : "Unhealthy",
        InstanceName = workerOptions.Value.InstanceName,
        CheckedAt = DateTimeOffset.UtcNow,
        Checks = new Dictionary<string, string>
        {
            ["application"] = applicationOptions.Value.Name,
            ["environment"] = applicationOptions.Value.Environment,
            ["kafka"] = kafkaHealthy ? "Healthy" : "Unhealthy",
            ["mongodb"] = mongoDbHealthy ? "Healthy" : "Unhealthy"
        }
    };

    return isHealthy
        ? Results.Ok(response)
        : Results.Json(response, statusCode: StatusCodes.Status503ServiceUnavailable);
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

public partial class Program;