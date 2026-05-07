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

var builder = WebApplication.CreateBuilder(args);

builder.Services
    .AddOptions<WorkerOptions>()
    .Bind(builder.Configuration.GetSection(WorkerOptions.SectionName))
    .Validate(options => !string.IsNullOrWhiteSpace(options.InstanceName), "Worker:InstanceName è obbligatorio.")
    .Validate(options => options.ErrorDelayInSeconds > 0, "Worker:ErrorDelayInSeconds deve essere maggiore di zero.")
    .Validate(options => options.ConsecutiveFailuresWarningThreshold > 0, "Worker:ConsecutiveFailuresWarningThreshold deve essere maggiore di zero.")
    .Validate(options => options.TechnicalRetryMaxAttempts > 0, "Worker:TechnicalRetryMaxAttempts deve essere maggiore di zero.")
    .Validate(options => options.TechnicalRetryInitialDelayInSeconds > 0, "Worker:TechnicalRetryInitialDelayInSeconds deve essere maggiore di zero.")
    .Validate(options => options.TechnicalRetryMaxDelayInSeconds > 0, "Worker:TechnicalRetryMaxDelayInSeconds deve essere maggiore di zero.")
    .Validate(
        options => options.TechnicalRetryMaxDelayInSeconds >= options.TechnicalRetryInitialDelayInSeconds,
        "Worker:TechnicalRetryMaxDelayInSeconds deve essere maggiore o uguale a Worker:TechnicalRetryInitialDelayInSeconds.")
    .ValidateOnStart();

builder.Services
    .AddOptions<KafkaOptions>()
    .Bind(builder.Configuration.GetSection(KafkaOptions.SectionName))
    .Validate(options => !string.IsNullOrWhiteSpace(options.BootstrapServers), "Kafka:BootstrapServers è obbligatorio.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.InputTopic), "Kafka:InputTopic è obbligatorio.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.DeadLetterTopic), "Kafka:DeadLetterTopic è obbligatorio.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.ConsumerGroupId), "Kafka:ConsumerGroupId è obbligatorio.")
    .Validate(
        options =>
        {
            var value = options.AutoOffsetReset.Trim().ToLowerInvariant();

            return value is "earliest" or "latest" or "error";
        },
        "Kafka:AutoOffsetReset deve essere Earliest, Latest oppure Error.")
    .ValidateOnStart();

builder.Services
    .AddOptions<MongoDbOptions>()
    .Bind(builder.Configuration.GetSection(MongoDbOptions.SectionName))
    .Validate(options => !string.IsNullOrWhiteSpace(options.ConnectionString), "MongoDb:ConnectionString è obbligatoria.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.DatabaseName), "MongoDb:DatabaseName è obbligatorio.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.RoutingRulesCollectionName), "MongoDb:RoutingRulesCollectionName è obbligatorio.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.ProcessedMessagesCollectionName), "MongoDb:ProcessedMessagesCollectionName è obbligatorio.")
    .ValidateOnStart();

builder.Services.AddSingleton<IKafkaMessageConsumer, KafkaMessageConsumer>();
builder.Services.AddSingleton<IKafkaMessageProducer, KafkaMessageProducer>();

builder.Services.AddSingleton<IEventEnvelopeParser, EventEnvelopeParser>();
builder.Services.AddSingleton<IEventRoutingService, MongoDbEventRoutingService>();
builder.Services.AddSingleton<IDeadLetterMessageFactory, DeadLetterMessageFactory>();

builder.Services.AddSingleton<IRoutingRuleRepository, RoutingRuleRepository>();
builder.Services.AddSingleton<IProcessedMessageRepository, ProcessedMessageRepository>();
builder.Services.AddSingleton<IMongoDbInitializer, MongoDbInitializer>();

builder.Services.AddSingleton<IKafkaHealthCheckService, KafkaHealthCheckService>();
builder.Services.AddSingleton<IMongoDbHealthCheckService, MongoDbHealthCheckService>();

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