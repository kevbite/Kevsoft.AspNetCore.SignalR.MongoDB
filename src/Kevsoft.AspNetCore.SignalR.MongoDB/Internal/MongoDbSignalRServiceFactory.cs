using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal static class MongoDbSignalRServiceFactory
{
    public static IMongoSignalRBackplane CreateBackplane(IServiceProvider serviceProvider)
    {
        var options = serviceProvider.GetRequiredService<IOptions<MongoDbSignalROptions>>().Value;
        var database = serviceProvider.GetRequiredService<IMongoDatabase>();
        var serializer = serviceProvider.GetRequiredService<IBackplaneEnvelopeSerializer>();
        var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();

        return options.TransportMode switch
        {
            MongoDbSignalRTransportMode.ChangeStreams => new MongoDbChangeStreamBackplane(
                database,
                options,
                serializer,
                loggerFactory.CreateLogger<MongoDbChangeStreamBackplane>()),
            MongoDbSignalRTransportMode.TailableAwait => new MongoDbTailableAwaitBackplane(
                database,
                options,
                serializer,
                loggerFactory.CreateLogger<MongoDbTailableAwaitBackplane>()),
            _ => throw new OptionsValidationException(
                Options.DefaultName,
                typeof(MongoDbSignalROptions),
                [$"TransportMode '{options.TransportMode}' is not supported."])
        };
    }
}
