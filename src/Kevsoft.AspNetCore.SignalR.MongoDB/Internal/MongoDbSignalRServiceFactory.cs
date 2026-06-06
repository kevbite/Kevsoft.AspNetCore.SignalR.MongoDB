using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal static class MongoDbSignalRServiceFactory
{
    public static IMongoSignalRBackplane CreateBackplane(IServiceProvider serviceProvider)
    {
        var options = serviceProvider.GetRequiredService<IOptions<MongoDbSignalROptions>>().Value;
        var database = MongoDbSignalRDatabaseFactory.CreateDatabase(serviceProvider, options);

        return options.TransportMode switch
        {
            MongoDbSignalRTransportMode.ChangeStreams => 
                ActivatorUtilities.CreateInstance<MongoDbChangeStreamBackplane>(
                    serviceProvider,
                    database,
                    options),
            MongoDbSignalRTransportMode.TailableAwait => 
                ActivatorUtilities.CreateInstance<MongoDbTailableAwaitBackplane>(
                    serviceProvider,
                    database,
                    options),
            _ => throw new OptionsValidationException(
                Options.DefaultName,
                typeof(MongoDbSignalROptions),
                [$"TransportMode '{options.TransportMode}' is not supported."])
        };
    }
}
