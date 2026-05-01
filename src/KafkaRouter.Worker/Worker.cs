using Confluent.Kafka;
using KafkaRouter.Worker.Kafka;
using KafkaRouter.Worker.Options;
using Microsoft.Extensions.Options;

namespace KafkaRouter.Worker;

public sealed class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IKafkaMessageConsumer _kafkaMessageConsumer;
    private readonly IKafkaMessageProducer _kafkaMessageProducer;
    private readonly WorkerOptions _workerOptions;
    private readonly KafkaOptions _kafkaOptions;

    public Worker(
        ILogger<Worker> logger,
        IKafkaMessageConsumer kafkaMessageConsumer,
        IKafkaMessageProducer kafkaMessageProducer,
        IOptions<WorkerOptions> workerOptions,
        IOptions<KafkaOptions> kafkaOptions)
    {
        _logger = logger;
        _kafkaMessageConsumer = kafkaMessageConsumer;
        _kafkaMessageProducer = kafkaMessageProducer;
        _workerOptions = workerOptions.Value;
        _kafkaOptions = kafkaOptions.Value;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Kafka Router Worker avviato.");

        _kafkaMessageConsumer.Subscribe();

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _kafkaMessageConsumer.Consume(stoppingToken);

                    LogConsumedMessage(consumeResult);

                    await ProduceToOutputTopicAsync(
                        consumeResult,
                        stoppingToken);

                    /*
                     * Ora il commit avviene solo DOPO che il messaggio
                     * è stato prodotto correttamente sul topic di output.
                     *
                     * Questo è il primo passo verso una semantica at-least-once.
                     */
                    _kafkaMessageConsumer.Commit(consumeResult);
                }
                catch (ConsumeException exception)
                {
                    _logger.LogError(
                        exception,
                        "Errore durante la lettura da Kafka. Attendo {DelayInSeconds} secondi prima di riprovare.",
                        _workerOptions.ErrorDelayInSeconds);

                    await DelayAfterErrorAsync(stoppingToken);
                }
                catch (ProduceException<string, string> exception)
                {
                    _logger.LogError(
                        exception,
                        "Errore durante la produzione su Kafka. Il messaggio non verrà committato. Attendo {DelayInSeconds} secondi prima di riprovare.",
                        _workerOptions.ErrorDelayInSeconds);

                    await DelayAfterErrorAsync(stoppingToken);
                }
                catch (KafkaException exception)
                {
                    _logger.LogError(
                        exception,
                        "Errore Kafka generico. Attendo {DelayInSeconds} secondi prima di riprovare.",
                        _workerOptions.ErrorDelayInSeconds);

                    await DelayAfterErrorAsync(stoppingToken);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("Richiesta di arresto ricevuta.");
        }
        finally
        {
            _logger.LogInformation("Kafka Router Worker arrestato correttamente.");
        }
    }

    private async Task ProduceToOutputTopicAsync(
        ConsumeResult<string, string> consumeResult,
        CancellationToken cancellationToken)
    {
        var key = consumeResult.Message.Key;
        var value = consumeResult.Message.Value;

        if (string.IsNullOrWhiteSpace(value))
        {
            _logger.LogWarning(
                "Messaggio Kafka ignorato perché il payload è vuoto. Topic: {Topic}. Partition: {Partition}. Offset: {Offset}.",
                consumeResult.Topic,
                consumeResult.Partition.Value,
                consumeResult.Offset.Value);

            return;
        }

        await _kafkaMessageProducer.ProduceAsync(
            _kafkaOptions.OutputTopic,
            key,
            value,
            cancellationToken);
    }

    private async Task DelayAfterErrorAsync(CancellationToken stoppingToken)
    {
        await Task.Delay(
            TimeSpan.FromSeconds(_workerOptions.ErrorDelayInSeconds),
            stoppingToken);
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