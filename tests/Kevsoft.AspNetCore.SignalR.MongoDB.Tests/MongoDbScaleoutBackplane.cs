namespace Kevsoft.AspNetCore.SignalR.MongoDB.Tests;

public sealed class MongoDbScaleoutBackplane : IAsyncDisposable
{
    internal FakeMongoSignalRBackplane Backplane { get; } = new();

    public ValueTask DisposeAsync()
    {
        return Backplane.DisposeAsync();
    }
}
