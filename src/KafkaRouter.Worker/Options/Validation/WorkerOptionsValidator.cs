using Microsoft.Extensions.Options;

namespace KafkaRouter.Worker.Options.Validation;

public sealed class WorkerOptionsValidator : IValidateOptions<WorkerOptions>
{
    public ValidateOptionsResult Validate(
        string? name,
        WorkerOptions options)
    {
        var errors = new List<string>();

        if (string.IsNullOrWhiteSpace(options.InstanceName))
        {
            errors.Add("Worker:InstanceName è obbligatorio.");
        }

        if (options.ErrorDelayInSeconds <= 0)
        {
            errors.Add("Worker:ErrorDelayInSeconds deve essere maggiore di zero.");
        }

        if (options.ConsecutiveFailuresWarningThreshold <= 0)
        {
            errors.Add("Worker:ConsecutiveFailuresWarningThreshold deve essere maggiore di zero.");
        }

        if (options.TechnicalRetryMaxAttempts <= 0)
        {
            errors.Add("Worker:TechnicalRetryMaxAttempts deve essere maggiore di zero.");
        }

        if (options.TechnicalRetryInitialDelayInSeconds <= 0)
        {
            errors.Add("Worker:TechnicalRetryInitialDelayInSeconds deve essere maggiore di zero.");
        }

        if (options.TechnicalRetryMaxDelayInSeconds <= 0)
        {
            errors.Add("Worker:TechnicalRetryMaxDelayInSeconds deve essere maggiore di zero.");
        }

        if (options.TechnicalRetryMaxDelayInSeconds < options.TechnicalRetryInitialDelayInSeconds)
        {
            errors.Add("Worker:TechnicalRetryMaxDelayInSeconds deve essere maggiore o uguale a Worker:TechnicalRetryInitialDelayInSeconds.");
        }

        return errors.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(errors);
    }
}