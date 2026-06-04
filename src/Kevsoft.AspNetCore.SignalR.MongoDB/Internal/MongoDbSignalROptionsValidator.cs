using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal sealed class MongoDbSignalROptionsValidator : IValidateOptions<MongoDbSignalROptions>
{
    public ValidateOptionsResult Validate(string? name, MongoDbSignalROptions options)
    {
        if (name != Options.DefaultName)
        {
            return ValidateOptionsResult.Skip;
        }

        var failures = new List<string>();

        if (options.MongoClientFactory == null && string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            failures.Add("Either ConnectionString or MongoClientFactory must be configured.");
        }

        if (!string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            try
            {
                _ = MongoUrl.Create(options.ConnectionString);
            }
            catch (Exception ex)
            {
                failures.Add($"ConnectionString is not a valid MongoDB connection string: {ex.Message}");
            }
        }

        if (string.IsNullOrWhiteSpace(options.DatabaseName))
        {
            failures.Add("DatabaseName must be configured.");
        }

        if (string.IsNullOrWhiteSpace(options.CollectionName))
        {
            failures.Add("CollectionName must be configured.");
        }

        if (!Enum.IsDefined(options.TransportMode))
        {
            failures.Add($"TransportMode '{options.TransportMode}' is not supported.");
        }

        if (options.AckTimeout <= TimeSpan.Zero)
        {
            failures.Add("AckTimeout must be greater than zero.");
        }

        if (options.TailableAwaitMaxAwaitTime <= TimeSpan.Zero)
        {
            failures.Add("TailableAwaitMaxAwaitTime must be greater than zero.");
        }

        if (options.TailableCollectionSizeBytes <= 0)
        {
            failures.Add("TailableCollectionSizeBytes must be greater than zero.");
        }

        if (options.MessageTtl <= TimeSpan.Zero)
        {
            failures.Add("MessageTtl must be greater than zero.");
        }

        if (options.ConnectionPresenceTtl <= TimeSpan.Zero)
        {
            failures.Add("ConnectionPresenceTtl must be greater than zero.");
        }

        return failures.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(failures);
    }
}
