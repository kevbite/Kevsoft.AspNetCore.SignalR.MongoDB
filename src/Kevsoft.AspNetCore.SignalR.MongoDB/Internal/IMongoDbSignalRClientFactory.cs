using MongoDB.Driver;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal interface IMongoDbSignalRClientFactory
{
    IMongoClient CreateClient();
}
