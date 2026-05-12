namespace KafkaRouter.Worker.Diagnostics;

public static class ConnectionStringSanitizer
{
    public static string SanitizeMongoDbConnectionString(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return string.Empty;
        }

        try
        {
            var schemeSeparatorIndex = connectionString.IndexOf(
                "://",
                StringComparison.Ordinal);

            if (schemeSeparatorIndex < 0)
            {
                return "***";
            }

            var scheme = connectionString[..(schemeSeparatorIndex + 3)];
            var remainder = connectionString[(schemeSeparatorIndex + 3)..];

            var atIndex = remainder.IndexOf('@', StringComparison.Ordinal);

            if (atIndex < 0)
            {
                return connectionString;
            }

            var hostAndPath = remainder[(atIndex + 1)..];

            return $"{scheme}***:***@{hostAndPath}";
        }
        catch
        {
            return "***";
        }
    }
}