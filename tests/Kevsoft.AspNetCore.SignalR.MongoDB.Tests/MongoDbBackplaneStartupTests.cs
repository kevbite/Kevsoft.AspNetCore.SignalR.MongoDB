using System.Net;
using System.Net.Sockets;
using Kevsoft.AspNetCore.SignalR.MongoDB.Internal;
using Microsoft.AspNetCore.SignalR.Protocol;
using Microsoft.Extensions.Logging.Abstractions;
using MongoDB.Driver;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Tests;

public class MongoDbBackplaneStartupTests
{
    [Theory]
    [InlineData(MongoDbSignalRTransportMode.ChangeStreams)]
    [InlineData(MongoDbSignalRTransportMode.TailableAwait)]
    public async Task StartAsyncFailsWhenReaderCannotStart(MongoDbSignalRTransportMode transportMode)
    {
        var database = CreateUnavailableDatabase();
        await using var backplane = CreateBackplane(transportMode, database);

        var startTask = backplane.StartAsync("startup-test", CancellationToken.None).AsTask();

        Assert.Same(startTask, await Task.WhenAny(startTask, Task.Delay(TimeSpan.FromSeconds(10))));
        var exception = await Assert.ThrowsAnyAsync<Exception>(() => startTask);
        Assert.IsNotType<OperationCanceledException>(exception);
    }

    private static IMongoDatabase CreateUnavailableDatabase()
    {
        var settings = new MongoClientSettings
        {
            Server = new MongoServerAddress("127.0.0.1", GetUnusedLoopbackPort()),
            ServerSelectionTimeout = TimeSpan.FromMilliseconds(250),
            ConnectTimeout = TimeSpan.FromMilliseconds(250),
            DirectConnection = true
        };

        return new MongoClient(settings).GetDatabase("signalr_startup_failure");
    }

    private static int GetUnusedLoopbackPort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static IMongoSignalRBackplane CreateBackplane(
        MongoDbSignalRTransportMode transportMode,
        IMongoDatabase database)
    {
        var options = new MongoDbSignalROptions
        {
            CollectionName = "messages_" + Guid.NewGuid().ToString("N"),
            TransportMode = transportMode,
            RunCollectionSetupOnStartup = false,
            TailableAwaitMaxAwaitTime = TimeSpan.FromMilliseconds(100)
        };
        var serializer = new BsonBackplaneEnvelopeSerializer(
            new TestHubProtocolResolver(new JsonHubProtocol()));

        return transportMode switch
        {
            MongoDbSignalRTransportMode.ChangeStreams => new MongoDbChangeStreamBackplane(
                database,
                options,
                serializer,
                NullLogger<MongoDbChangeStreamBackplane>.Instance),
            MongoDbSignalRTransportMode.TailableAwait => new MongoDbTailableAwaitBackplane(
                database,
                options,
                serializer,
                NullLogger<MongoDbTailableAwaitBackplane>.Instance),
            _ => throw new ArgumentOutOfRangeException(nameof(transportMode), transportMode, null)
        };
    }
}
