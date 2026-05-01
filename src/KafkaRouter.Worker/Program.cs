using KafkaRouter.Worker;
using KafkaRouter.Worker.Options;

var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddOptions<WorkerOptions>()
    .Bind(builder.Configuration.GetSection(WorkerOptions.SectionName))
    .Validate(options => options.DelayInSeconds > 0, "Worker:DelayInSeconds deve essere maggiore di zero.")
    .ValidateOnStart();

builder.Services.AddHostedService<Worker>();

var host = builder.Build();

host.Run();