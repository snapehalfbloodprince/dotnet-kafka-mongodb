using Microsoft.Extensions.Options;

namespace KafkaRouter.Worker.Options.Validation;

public sealed class KafkaOptionsValidator : IValidateOptions<KafkaOptions>
{
    private static readonly HashSet<string> AllowedAutoOffsetResetValues = new(
        StringComparer.OrdinalIgnoreCase)
    {
        "earliest",
        "latest",
        "error"
    };

    public ValidateOptionsResult Validate(
        string? name,
        KafkaOptions options)
    {
        var errors = new List<string>();

        if (string.IsNullOrWhiteSpace(options.BootstrapServers))
        {
            errors.Add("Kafka:BootstrapServers è obbligatorio.");
        }

        if (string.IsNullOrWhiteSpace(options.InputTopic))
        {
            errors.Add("Kafka:InputTopic è obbligatorio.");
        }

        if (string.IsNullOrWhiteSpace(options.DeadLetterTopic))
        {
            errors.Add("Kafka:DeadLetterTopic è obbligatorio.");
        }

        if (string.IsNullOrWhiteSpace(options.ConsumerGroupId))
        {
            errors.Add("Kafka:ConsumerGroupId è obbligatorio.");
        }

        if (string.IsNullOrWhiteSpace(options.AutoOffsetReset))
        {
            errors.Add("Kafka:AutoOffsetReset è obbligatorio.");
        }
        else if (!AllowedAutoOffsetResetValues.Contains(options.AutoOffsetReset.Trim()))
        {
            errors.Add("Kafka:AutoOffsetReset deve essere Earliest, Latest oppure Error.");
        }

        if (!string.IsNullOrWhiteSpace(options.InputTopic)
            && !string.IsNullOrWhiteSpace(options.DeadLetterTopic)
            && string.Equals(
                options.InputTopic.Trim(),
                options.DeadLetterTopic.Trim(),
                StringComparison.OrdinalIgnoreCase))
        {
            errors.Add("Kafka:InputTopic e Kafka:DeadLetterTopic non possono coincidere.");
        }

        return errors.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(errors);
    }
}