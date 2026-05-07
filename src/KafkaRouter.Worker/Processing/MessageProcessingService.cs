using Confluent.Kafka;
using KafkaRouter.Worker.DeadLetter;
using KafkaRouter.Worker.Kafka;
using KafkaRouter.Worker.Metrics;
using KafkaRouter.Worker.Models;
using KafkaRouter.Worker.MongoDb.Documents;
using KafkaRouter.Worker.MongoDb.Repositories;
using KafkaRouter.Worker.Options;
using KafkaRouter.Worker.Parsing;
using KafkaRouter.Worker.Routing;
using Microsoft.Extensions.Options;

namespace KafkaRouter.Worker.Processing;

public sealed class MessageProcessingService : IMessageProcessingService
{
    private readonly ILogger<MessageProcessingService> _logger;
    private readonly IKafkaMessageConsumer _kafkaMessageConsumer;
    private readonly IKafkaMessageProducer _kafkaMessageProducer;
    private readonly IEventEnvelopeParser _eventEnvelopeParser;
    private readonly IEventRoutingService _eventRoutingService;
    private readonly IDeadLetterMessageFactory _deadLetterMessageFactory;
    private readonly IProcessedMessageRepository _processedMessageRepository;
    private readonly IWorkerMetrics _workerMetrics;
    private readonly KafkaOptions _kafkaOptions;

    public MessageProcessingService(
        ILogger<MessageProcessingService> logger,
        IKafkaMessageConsumer kafkaMessageConsumer,
        IKafkaMessageProducer kafkaMessageProducer,
        IEventEnvelopeParser eventEnvelopeParser,
        IEventRoutingService eventRoutingService,
        IDeadLetterMessageFactory deadLetterMessageFactory,
        IProcessedMessageRepository processedMessageRepository,
        IWorkerMetrics workerMetrics,
        IOptions<KafkaOptions> kafkaOptions)
    {
        _logger = logger;
        _kafkaMessageConsumer = kafkaMessageConsumer;
        _kafkaMessageProducer = kafkaMessageProducer;
        _eventEnvelopeParser = eventEnvelopeParser;
        _eventRoutingService = eventRoutingService;
        _deadLetterMessageFactory = deadLetterMessageFactory;
        _processedMessageRepository = processedMessageRepository;
        _workerMetrics = workerMetrics;
        _kafkaOptions = kafkaOptions.Value;
    }

    public async Task ProcessAsync(
        ConsumeResult<string, string> consumeResult,
        CancellationToken cancellationToken)
    {
        LogConsumedMessage(consumeResult);

        var parseResult = _eventEnvelopeParser.Parse(consumeResult.Message.Value);

        if (!parseResult.IsSuccess)
        {
            await ProduceToDeadLetterTopicAsync(
                consumeResult,
                parseResult.ErrorCode ?? "PARSE_ERROR",
                parseResult.ErrorMessage ?? "Errore non specificato durante il parsing del messaggio.",
                eventEnvelope: null,
                cancellationToken);

            _kafkaMessageConsumer.Commit(consumeResult);

            LogApplicationFailureHandled(
                consumeResult,
                parseResult.ErrorCode ?? "PARSE_ERROR",
                parseResult.ErrorMessage ?? "Errore non specificato durante il parsing del messaggio.");

            return;
        }

        var eventEnvelope = parseResult.EventEnvelope!;

        var alreadyProcessed = await _processedMessageRepository.ExistsByEventIdAsync(
            eventEnvelope.EventId!,
            cancellationToken);

        if (alreadyProcessed)
        {
            _workerMetrics.IncrementDuplicateMessages(
                eventEnvelope.EventId!,
                eventEnvelope.EventType!);

            _logger.LogWarning(
                "Messaggio duplicato rilevato. EventId: {EventId}. EventType: {EventType}. Topic: {Topic}. Partition: {Partition}. Offset: {Offset}. Il messaggio non verrà riprodotto sui topic di destinazione.",
                eventEnvelope.EventId,
                eventEnvelope.EventType,
                consumeResult.Topic,
                consumeResult.Partition.Value,
                consumeResult.Offset.Value);

            _kafkaMessageConsumer.Commit(consumeResult);

            return;
        }

        var routingDecision = await _eventRoutingService.GetRoutingDecisionAsync(
            eventEnvelope,
            cancellationToken);

        if (!routingDecision.IsRoutable)
        {
            await ProduceToDeadLetterTopicAsync(
                consumeResult,
                routingDecision.ErrorCode ?? "ROUTING_ERROR",
                routingDecision.ErrorMessage ?? "Errore non specificato durante il routing del messaggio.",
                eventEnvelope,
                cancellationToken);

            _kafkaMessageConsumer.Commit(consumeResult);

            LogApplicationFailureHandled(
                consumeResult,
                routingDecision.ErrorCode ?? "ROUTING_ERROR",
                routingDecision.ErrorMessage ?? "Errore non specificato durante il routing del messaggio.");

            return;
        }

        await ProduceToDestinationTopicsAsync(
            consumeResult,
            eventEnvelope,
            routingDecision,
            cancellationToken);

        var processedMessage = CreateProcessedMessageDocument(
            consumeResult,
            eventEnvelope,
            routingDecision);

        var inserted = await _processedMessageRepository.TryInsertAsync(
            processedMessage,
            cancellationToken);

        if (!inserted)
        {
            _logger.LogWarning(
                "Il messaggio risulta già registrato come processato dopo la produzione. EventId: {EventId}. Possibile duplicato concorrente.",
                eventEnvelope.EventId);
        }

        _workerMetrics.IncrementProcessedMessages(
            eventEnvelope.EventId!,
            eventEnvelope.EventType!);

        _kafkaMessageConsumer.Commit(consumeResult);
    }

    private async Task ProduceToDestinationTopicsAsync(
        ConsumeResult<string, string> consumeResult,
        EventEnvelope eventEnvelope,
        RoutingDecision routingDecision,
        CancellationToken cancellationToken)
    {
        var effectiveKey = GetEffectiveMessageKey(
            consumeResult,
            eventEnvelope);

        foreach (var destinationTopic in routingDecision.DestinationTopics)
        {
            _logger.LogInformation(
                "Produzione evento verso topic destinazione. EventId: {EventId}. EventType: {EventType}. DestinationTopic: {DestinationTopic}.",
                eventEnvelope.EventId,
                eventEnvelope.EventType,
                destinationTopic);

            await _kafkaMessageProducer.ProduceAsync(
                destinationTopic,
                effectiveKey,
                consumeResult.Message.Value,
                cancellationToken);
        }

        _logger.LogInformation(
            "Evento instradato correttamente. EventId: {EventId}. EventType: {EventType}. DestinationTopics: {DestinationTopics}.",
            eventEnvelope.EventId,
            eventEnvelope.EventType,
            string.Join(", ", routingDecision.DestinationTopics));
    }

    private async Task ProduceToDeadLetterTopicAsync(
        ConsumeResult<string, string> consumeResult,
        string errorCode,
        string errorMessage,
        EventEnvelope? eventEnvelope,
        CancellationToken cancellationToken)
    {
        var deadLetterPayload = _deadLetterMessageFactory.CreateDeadLetterPayload(
            consumeResult,
            errorCode,
            errorMessage,
            eventEnvelope);

        var deadLetterKey = eventEnvelope?.EventId
            ?? consumeResult.Message.Key
            ?? $"{consumeResult.Topic}-{consumeResult.Partition.Value}-{consumeResult.Offset.Value}";

        _logger.LogWarning(
            "Messaggio inviato in DLQ. ErrorCode: {ErrorCode}. ErrorMessage: {ErrorMessage}. DeadLetterTopic: {DeadLetterTopic}. OriginalTopic: {OriginalTopic}. OriginalPartition: {OriginalPartition}. OriginalOffset: {OriginalOffset}.",
            errorCode,
            errorMessage,
            _kafkaOptions.DeadLetterTopic,
            consumeResult.Topic,
            consumeResult.Partition.Value,
            consumeResult.Offset.Value);

        _workerMetrics.IncrementDeadLetterMessages(
            eventEnvelope?.EventId,
            eventEnvelope?.EventType,
            errorCode);

        await _kafkaMessageProducer.ProduceAsync(
            _kafkaOptions.DeadLetterTopic,
            deadLetterKey,
            deadLetterPayload,
            cancellationToken);
    }

    private static ProcessedMessageDocument CreateProcessedMessageDocument(
        ConsumeResult<string, string> consumeResult,
        EventEnvelope eventEnvelope,
        RoutingDecision routingDecision)
    {
        return new ProcessedMessageDocument
        {
            EventId = eventEnvelope.EventId!,
            EventType = eventEnvelope.EventType!,
            SourceTopic = consumeResult.Topic,
            SourcePartition = consumeResult.Partition.Value,
            SourceOffset = consumeResult.Offset.Value,
            DestinationTopics = routingDecision.DestinationTopics.ToArray(),
            ProcessedAt = DateTimeOffset.UtcNow
        };
    }

    private void LogApplicationFailureHandled(
        ConsumeResult<string, string> consumeResult,
        string errorCode,
        string errorMessage)
    {
        _logger.LogWarning(
            "Errore applicativo gestito con DLQ e commit. ErrorCode: {ErrorCode}. ErrorMessage: {ErrorMessage}. Topic: {Topic}. Partition: {Partition}. Offset: {Offset}.",
            errorCode,
            errorMessage,
            consumeResult.Topic,
            consumeResult.Partition.Value,
            consumeResult.Offset.Value);
    }

    private static string? GetEffectiveMessageKey(
        ConsumeResult<string, string> consumeResult,
        EventEnvelope eventEnvelope)
    {
        if (!string.IsNullOrWhiteSpace(consumeResult.Message.Key))
        {
            return consumeResult.Message.Key;
        }

        if (!string.IsNullOrWhiteSpace(eventEnvelope.EventId))
        {
            return eventEnvelope.EventId;
        }

        return null;
    }

    private void LogConsumedMessage(ConsumeResult<string, string> consumeResult)
    {
        _logger.LogInformation(
            """
            Messaggio Kafka ricevuto.
            Topic: {Topic}
            Partition: {Partition}
            Offset: {Offset}
            Key: {Key}
            Value: {Value}
            """,
            consumeResult.Topic,
            consumeResult.Partition.Value,
            consumeResult.Offset.Value,
            consumeResult.Message.Key,
            consumeResult.Message.Value);
    }
}