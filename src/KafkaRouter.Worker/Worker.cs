using Confluent.Kafka;
using KafkaRouter.Worker.Kafka;
using KafkaRouter.Worker.Options;
using Microsoft.Extensions.Options;

namespace KafkaRouter.Worker;

public sealed class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IKafkaMessageConsumer _kafkaMessageConsumer;
    private readonly WorkerOptions _workerOptions;

    public Worker(
        ILogger<Worker> logger,
        IKafkaMessageConsumer kafkaMessageConsumer,
        IOptions<WorkerOptions> workerOptions)
    {
        _logger = logger;
        _kafkaMessageConsumer = kafkaMessageConsumer;
        _workerOptions = workerOptions.Value;
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

                    /*
                     * Per ora il processamento è semplicemente:
                     * "ho letto il messaggio e l'ho scritto nei log".
                     *
                     * Quindi possiamo committare.
                     *
                     * Nelle prossime lezioni il commit avverrà solo dopo:
                     * - validazione JSON
                     * - routing verso N topic
                     * - eventuale scrittura audit su MongoDB
                     * - gestione DLQ se necessaria
                     */
                    _kafkaMessageConsumer.Commit(consumeResult);
                }
                catch (ConsumeException exception)
                {
                    _logger.LogError(
                        exception,
                        "Errore durante la lettura da Kafka. Attendo {DelayInSeconds} secondi prima di riprovare.",
                        _workerOptions.ErrorDelayInSeconds);

                    await Task.Delay(
                        TimeSpan.FromSeconds(_workerOptions.ErrorDelayInSeconds),
                        stoppingToken);
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