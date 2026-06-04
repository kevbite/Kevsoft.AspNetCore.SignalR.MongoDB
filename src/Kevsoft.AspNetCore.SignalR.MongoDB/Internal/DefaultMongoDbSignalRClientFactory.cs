using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal sealed class DefaultMongoDbSignalRClientFactory(
    IServiceProvider serviceProvider,
    IOptions<MongoDbSignalROptions> options) : IMongoDbSignalRClientFactory
{
    public IMongoClient CreateClient()
    {
        var value = options.Value;
        if (value.MongoClientFactory != null)
        {
            return value.MongoClientFactory(serviceProvider);
        }

        if (string.IsNullOrWhiteSpace(value.ConnectionString))
        {
            throw new InvalidOperationException(
                "MongoDB SignalR requires either a connection string or MongoClientFactory.");
        }

        var settings = MongoClientSettings.FromConnectionString(value.ConnectionString);
        value.ConfigureClientSettings?.Invoke(settings);
        return new MongoClient(settings);
    }
}
