using MongoDB.Driver;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal static class MongoDbSignalRDatabaseFactory
{
    public static IMongoDatabase CreateDatabase(IServiceProvider serviceProvider, MongoDbSignalROptions options)
    {
        if (options.MongoDatabaseFactory is not null)
        {
            return options.MongoDatabaseFactory(serviceProvider);
        }

        var client = CreateClient(serviceProvider, options);
        if (string.IsNullOrWhiteSpace(options.DatabaseName))
        {
            throw new InvalidOperationException(
                "DatabaseName must be configured when using ConnectionString or MongoClientFactory.");
        }

        return client.GetDatabase(options.DatabaseName);
    }

    private static IMongoClient CreateClient(IServiceProvider serviceProvider, MongoDbSignalROptions options)
    {
        if (options.MongoClientFactory is not null)
        {
            return options.MongoClientFactory(serviceProvider);
        }

        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            throw new InvalidOperationException(
                "MongoDB SignalR requires either a connection string, MongoClientFactory, or MongoDatabaseFactory.");
        }

        var settings = MongoClientSettings.FromConnectionString(options.ConnectionString);
        options.ConfigureClientSettings?.Invoke(settings);
        return new MongoClient(settings);
    }
}


