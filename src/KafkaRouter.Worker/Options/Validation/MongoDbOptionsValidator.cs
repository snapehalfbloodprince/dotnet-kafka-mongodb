using Microsoft.Extensions.Options;

namespace KafkaRouter.Worker.Options.Validation;

public sealed class MongoDbOptionsValidator : IValidateOptions<MongoDbOptions>
{
    public ValidateOptionsResult Validate(
        string? name,
        MongoDbOptions options)
    {
        var errors = new List<string>();

        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            errors.Add("MongoDb:ConnectionString è obbligatoria.");
        }
        else if (!Uri.TryCreate(options.ConnectionString, UriKind.Absolute, out var uri)
                 || uri.Scheme is not ("mongodb" or "mongodb+srv"))
        {
            errors.Add("MongoDb:ConnectionString deve essere una URI MongoDB valida.");
        }

        if (string.IsNullOrWhiteSpace(options.DatabaseName))
        {
            errors.Add("MongoDb:DatabaseName è obbligatorio.");
        }

        if (string.IsNullOrWhiteSpace(options.RoutingRulesCollectionName))
        {
            errors.Add("MongoDb:RoutingRulesCollectionName è obbligatorio.");
        }

        if (string.IsNullOrWhiteSpace(options.ProcessedMessagesCollectionName))
        {
            errors.Add("MongoDb:ProcessedMessagesCollectionName è obbligatorio.");
        }

        if (!string.IsNullOrWhiteSpace(options.RoutingRulesCollectionName)
            && !string.IsNullOrWhiteSpace(options.ProcessedMessagesCollectionName)
            && string.Equals(
                options.RoutingRulesCollectionName.Trim(),
                options.ProcessedMessagesCollectionName.Trim(),
                StringComparison.OrdinalIgnoreCase))
        {
            errors.Add("MongoDb:RoutingRulesCollectionName e MongoDb:ProcessedMessagesCollectionName non possono coincidere.");
        }

        return errors.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(errors);
    }
}