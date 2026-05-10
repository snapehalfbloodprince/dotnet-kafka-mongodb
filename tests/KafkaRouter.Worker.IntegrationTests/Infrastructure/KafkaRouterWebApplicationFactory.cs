using KafkaRouter.Worker.Health;
using KafkaRouter.Worker.Kafka;
using KafkaRouter.Worker.Metrics;
using KafkaRouter.Worker.MongoDb.Repositories;
using KafkaRouter.Worker.Startup;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Moq;
using Microsoft.Extensions.Configuration;

namespace KafkaRouter.Worker.IntegrationTests.Infrastructure;

public sealed class KafkaRouterWebApplicationFactory : WebApplicationFactory<Program>
{
    private readonly bool _isKafkaHealthy;
    private readonly bool _isMongoDbHealthy;

    public KafkaRouterWebApplicationFactory(
        bool isKafkaHealthy = true,
        bool isMongoDbHealthy = true)
    {
        _isKafkaHealthy = isKafkaHealthy;
        _isMongoDbHealthy = isMongoDbHealthy;
    }

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Testing");

        builder.ConfigureAppConfiguration((_, configurationBuilder) =>
        {
            var configurationOverrides = new Dictionary<string, string?>
            {
                ["Application:Environment"] = "Testing"
            };

            configurationBuilder.AddInMemoryCollection(configurationOverrides);
        });

        builder.ConfigureServices(services =>
        {
            services.RemoveAll<IKafkaMessageConsumer>();
            services.RemoveAll<IKafkaMessageProducer>();

            services.RemoveAll<IRoutingRuleRepository>();
            services.RemoveAll<IProcessedMessageRepository>();
            services.RemoveAll<IMongoDbInitializer>();

            services.RemoveAll<IKafkaHealthCheckService>();
            services.RemoveAll<IMongoDbHealthCheckService>();

            services.RemoveAll<IHostedService>();

            services.AddSingleton(CreateKafkaConsumerMock().Object);
            services.AddSingleton(CreateKafkaProducerMock().Object);

            services.AddSingleton(CreateRoutingRuleRepositoryMock().Object);
            services.AddSingleton(CreateProcessedMessageRepositoryMock().Object);
            services.AddSingleton(CreateMongoDbInitializerMock().Object);

            services.AddSingleton(CreateKafkaHealthCheckServiceMock().Object);
            services.AddSingleton(CreateMongoDbHealthCheckServiceMock().Object);
        });
    }

    private static Mock<IKafkaMessageConsumer> CreateKafkaConsumerMock()
    {
        var mock = new Mock<IKafkaMessageConsumer>();

        mock.Setup(consumer => consumer.Subscribe());

        return mock;
    }

    private static Mock<IKafkaMessageProducer> CreateKafkaProducerMock()
    {
        return new Mock<IKafkaMessageProducer>();
    }

    private static Mock<IRoutingRuleRepository> CreateRoutingRuleRepositoryMock()
    {
        return new Mock<IRoutingRuleRepository>();
    }

    private static Mock<IProcessedMessageRepository> CreateProcessedMessageRepositoryMock()
    {
        return new Mock<IProcessedMessageRepository>();
    }

    private static Mock<IMongoDbInitializer> CreateMongoDbInitializerMock()
    {
        var mock = new Mock<IMongoDbInitializer>();

        mock.Setup(initializer => initializer.InitializeAsync(It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        return mock;
    }

    private Mock<IKafkaHealthCheckService> CreateKafkaHealthCheckServiceMock()
    {
        var mock = new Mock<IKafkaHealthCheckService>();

        mock.Setup(service => service.IsHealthyAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(_isKafkaHealthy);

        return mock;
    }

    private Mock<IMongoDbHealthCheckService> CreateMongoDbHealthCheckServiceMock()
    {
        var mock = new Mock<IMongoDbHealthCheckService>();

        mock.Setup(service => service.IsHealthyAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(_isMongoDbHealthy);

        return mock;
    }
}