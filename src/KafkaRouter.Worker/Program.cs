using KafkaRouter.Worker;
using KafkaRouter.Worker.Kafka;
using KafkaRouter.Worker.Options;

var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddOptions<WorkerOptions>()
    .Bind(builder.Configuration.GetSection(WorkerOptions.SectionName))
    .Validate(options => options.ErrorDelayInSeconds > 0, "Worker:ErrorDelayInSeconds deve essere maggiore di zero.")
    .ValidateOnStart();

builder.Services
    .AddOptions<KafkaOptions>()
    .Bind(builder.Configuration.GetSection(KafkaOptions.SectionName))
    .Validate(options => !string.IsNullOrWhiteSpace(options.BootstrapServers), "Kafka:BootstrapServers è obbligatorio.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.InputTopic), "Kafka:InputTopic è obbligatorio.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.OutputTopic), "Kafka:OutputTopic è obbligatorio.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.ConsumerGroupId), "Kafka:ConsumerGroupId è obbligatorio.")
    .Validate(
        options =>
        {
            var value = options.AutoOffsetReset.Trim().ToLowerInvariant();

            return value is "earliest" or "latest" or "error";
        },
        "Kafka:AutoOffsetReset deve essere Earliest, Latest oppure Error.")
    .ValidateOnStart();

builder.Services.AddSingleton<IKafkaMessageConsumer, KafkaMessageConsumer>();
builder.Services.AddSingleton<IKafkaMessageProducer, KafkaMessageProducer>();

builder.Services.AddHostedService<Worker>();

var host = builder.Build();

host.Run();