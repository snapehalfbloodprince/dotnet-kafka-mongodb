using KafkaRouter.Worker.Options;
using KafkaRouter.Worker.Options.Validation;
using Microsoft.Extensions.Options;

namespace KafkaRouter.Worker.DependencyInjection;

public static class OptionsServiceCollectionExtensions
{
    public static IServiceCollection AddValidatedOptions(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services
            .AddOptions<ApplicationOptions>()
            .Bind(configuration.GetSection(ApplicationOptions.SectionName))
            .ValidateOnStart();

        services
            .AddOptions<WorkerOptions>()
            .Bind(configuration.GetSection(WorkerOptions.SectionName))
            .ValidateOnStart();

        services
            .AddOptions<KafkaOptions>()
            .Bind(configuration.GetSection(KafkaOptions.SectionName))
            .ValidateOnStart();

        services
            .AddOptions<MongoDbOptions>()
            .Bind(configuration.GetSection(MongoDbOptions.SectionName))
            .ValidateOnStart();

        services.AddSingleton<IValidateOptions<ApplicationOptions>, ApplicationOptionsValidator>();
        services.AddSingleton<IValidateOptions<WorkerOptions>, WorkerOptionsValidator>();
        services.AddSingleton<IValidateOptions<KafkaOptions>, KafkaOptionsValidator>();
        services.AddSingleton<IValidateOptions<MongoDbOptions>, MongoDbOptionsValidator>();

        return services;
    }
}