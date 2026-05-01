using Confluent.Kafka;
using KafkaRouter.Worker.Options;
using Microsoft.Extensions.Options;

namespace KafkaRouter.Worker.Kafka;

public sealed class KafkaMessageProducer : IKafkaMessageProducer
{
    private readonly ILogger<KafkaMessageProducer> _logger;
    private readonly IProducer<string, string> _producer;

    public KafkaMessageProducer(
        IOptions<KafkaOptions> options,
        ILogger<KafkaMessageProducer> logger)
    {
        _logger = logger;

        var kafkaOptions = options.Value;

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = kafkaOptions.BootstrapServers,

            /*
             * Acks.All significa:
             * considera il messaggio consegnato solo quando Kafka conferma
             * secondo il livello di replica configurato.
             *
             * In locale abbiamo replication-factor 1, quindi è semplice.
             * In produzione diventa molto più importante.
             */
            Acks = Acks.All,

            /*
             * EnableIdempotence riduce il rischio di duplicati generati dal producer
             * in caso di retry interni del client.
             *
             * Non risolve da solo TUTTI i duplicati applicativi, ma è una buona base.
             */
            EnableIdempotence = true,

            ClientId = "kafka-router-worker-producer"
        };

        _producer = new ProducerBuilder<string, string>(producerConfig)
            .SetErrorHandler((_, error) =>
            {
                _logger.LogError(
                    "Errore Kafka producer. Code: {Code}. Reason: {Reason}. IsFatal: {IsFatal}",
                    error.Code,
                    error.Reason,
                    error.IsFatal);
            })
            .Build();
    }

    public async Task<DeliveryResult<string, string>> ProduceAsync(
        string topic,
        string? key,
        string value,
        CancellationToken cancellationToken)
    {
        var message = new Message<string, string>
        {
            Key = key,
            Value = value
        };

        var deliveryResult = await _producer.ProduceAsync(
            topic,
            message,
            cancellationToken);

        _logger.LogInformation(
            "Messaggio prodotto su Kafka. Topic: {Topic}. Partition: {Partition}. Offset: {Offset}. Status: {Status}.",
            deliveryResult.Topic,
            deliveryResult.Partition.Value,
            deliveryResult.Offset.Value,
            deliveryResult.Status);

        return deliveryResult;
    }

    public void Dispose()
    {
        try
        {
            _logger.LogInformation("Flush producer Kafka in corso.");

            /*
             * Flush tenta di completare eventuali messaggi ancora in coda
             * prima della chiusura del producer.
             */
            _producer.Flush(TimeSpan.FromSeconds(10));

            _logger.LogInformation("Producer Kafka flush completato.");
        }
        catch (KafkaException exception)
        {
            _logger.LogError(
                exception,
                "Errore durante il flush del producer Kafka.");
        }
        finally
        {
            _producer.Dispose();
        }
    }
}