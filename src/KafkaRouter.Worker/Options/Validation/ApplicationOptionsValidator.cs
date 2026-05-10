using Microsoft.Extensions.Options;

namespace KafkaRouter.Worker.Options.Validation;

public sealed class ApplicationOptionsValidator : IValidateOptions<ApplicationOptions>
{
    public ValidateOptionsResult Validate(
        string? name,
        ApplicationOptions options)
    {
        var errors = new List<string>();

        if (string.IsNullOrWhiteSpace(options.Name))
        {
            errors.Add("Application:Name è obbligatorio.");
        }

        if (string.IsNullOrWhiteSpace(options.Environment))
        {
            errors.Add("Application:Environment è obbligatorio.");
        }

        return errors.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(errors);
    }
}