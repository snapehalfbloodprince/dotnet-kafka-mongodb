using KafkaRouter.Worker.DeadLetter;
using KafkaRouter.Worker.Kafka;
using KafkaRouter.Worker.Metrics;
using KafkaRouter.Worker.MongoDb.Repositories;
using KafkaRouter.Worker.Options;
using KafkaRouter.Worker.Parsing;
using KafkaRouter.Worker.Processing;
using KafkaRouter.Worker.Routing;
using Microsoft.Extensions.Logging.Abstractions;

namespace KafkaRouter.Worker.IntegrationTests.EndToEnd.Infrastructure;

public static class EndToEndComponentFactory
{
    public static KafkaOptions CreateKafkaOptions(
        KafkaMongoDbTestcontainerFixture fixture,
        string inputTopic,
        string deadLetterTopic,
        string consumerGroupId)
    {
        return new KafkaOptions
        {
            BootstrapServers = fixture.KafkaBootstrapServers,
            InputTopic = inputTopic,
            DeadLetterTopic = deadLetterTopic,
            ConsumerGroupId = consumerGroupId,
            AutoOffsetReset = "Earliest"
        };
    }

    public static MongoDbOptions CreateMongoDbOptions(
        KafkaMongoDbTestcontainerFixture fixture)
    {
        return new MongoDbOptions
        {
            ConnectionString = fixture.MongoDbConnectionString,
            DatabaseName = fixture.DatabaseName,
            RoutingRulesCollectionName = "routing_rules",
            ProcessedMessagesCollectionName = "processed_messages"
        };
    }

    public static WorkerOptions CreateWorkerOptions()
    {
        return new WorkerOptions
        {
            InstanceName = $"e2e-worker-{Guid.NewGuid():N}",
            ErrorDelayInSeconds = 1,
            ConsecutiveFailuresWarningThreshold = 3,
            TechnicalRetryMaxAttempts = 1,
            TechnicalRetryInitialDelayInSeconds = 1,
            TechnicalRetryMaxDelayInSeconds = 1,
            ShutdownTimeoutInSeconds = 10
        };
    }

    public static RoutingRuleRepository CreateRoutingRuleRepository(
        KafkaMongoDbTestcontainerFixture fixture)
    {
        var mongoDbOptions = Microsoft.Extensions.Options.Options.Create(
            CreateMongoDbOptions(fixture));

        return new RoutingRuleRepository(
            mongoDbOptions,
            NullLogger<RoutingRuleRepository>.Instance);
    }

    public static ProcessedMessageRepository CreateProcessedMessageRepository(
        KafkaMongoDbTestcontainerFixture fixture)
    {
        var mongoDbOptions = Microsoft.Extensions.Options.Options.Create(
            CreateMongoDbOptions(fixture));

        return new ProcessedMessageRepository(
            mongoDbOptions,
            NullLogger<ProcessedMessageRepository>.Instance);
    }

    public static KafkaMessageConsumer CreateKafkaMessageConsumer(
    KafkaOptions kafkaOptions,
    WorkerOptions workerOptions)
{
    return new KafkaMessageConsumer(
        Microsoft.Extensions.Options.Options.Create(kafkaOptions),
        Microsoft.Extensions.Options.Options.Create(workerOptions),
        NullLogger<KafkaMessageConsumer>.Instance);
}

    public static KafkaMessageProducer CreateKafkaMessageProducer(
    KafkaOptions kafkaOptions,
    WorkerOptions workerOptions)
{
    return new KafkaMessageProducer(
        Microsoft.Extensions.Options.Options.Create(kafkaOptions),
        Microsoft.Extensions.Options.Options.Create(workerOptions),
        NullLogger<KafkaMessageProducer>.Instance);
}

    public static MessageProcessingService CreateMessageProcessingService(
        KafkaOptions kafkaOptions,
        WorkerOptions workerOptions,
        IKafkaMessageConsumer kafkaMessageConsumer,
        IKafkaMessageProducer kafkaMessageProducer,
        IRoutingRuleRepository routingRuleRepository,
        IProcessedMessageRepository processedMessageRepository,
        IWorkerMetrics workerMetrics)
    {
        return new MessageProcessingService(
            NullLogger<MessageProcessingService>.Instance,
            kafkaMessageConsumer,
            kafkaMessageProducer,
            new EventEnvelopeParser(),
            new MongoDbEventRoutingService(
                NullLogger<MongoDbEventRoutingService>.Instance,
                routingRuleRepository),
            new DeadLetterMessageFactory(),
            processedMessageRepository,
            workerMetrics,
            Microsoft.Extensions.Options.Options.Create(kafkaOptions));
    }

    public static InMemoryWorkerMetrics CreateWorkerMetrics(
        WorkerOptions workerOptions)
    {
        return new InMemoryWorkerMetrics(
            Microsoft.Extensions.Options.Options.Create(workerOptions));
    }
}