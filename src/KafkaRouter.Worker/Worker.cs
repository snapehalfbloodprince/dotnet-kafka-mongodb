using KafkaRouter.Worker.Options;
using Microsoft.Extensions.Options;

namespace KafkaRouter.Worker;

public sealed class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly WorkerOptions _options;

    public Worker(
        ILogger<Worker> logger,
        IOptions<WorkerOptions> options)
    {
        _logger = logger;
        _options = options.Value;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Kafka Router Worker avviato.");
        _logger.LogInformation(
            "Delay configurato: {DelayInSeconds} secondi.",
            _options.DelayInSeconds);

        var executionNumber = 0;

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                executionNumber++;

                _logger.LogInformation(
                    "Esecuzione numero {ExecutionNumber}. Il worker è vivo.",
                    executionNumber);

                await Task.Delay(
                    TimeSpan.FromSeconds(_options.DelayInSeconds),
                    stoppingToken);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Richiesta di arresto ricevuta.");
        }
        finally
        {
            _logger.LogInformation("Kafka Router Worker arrestato correttamente.");
        }
    }
}