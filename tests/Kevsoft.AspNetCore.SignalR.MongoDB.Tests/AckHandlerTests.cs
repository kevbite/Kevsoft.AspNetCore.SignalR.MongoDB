using Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Tests;

public class AckHandlerTests
{
    [Fact]
    public async Task TriggerAckCompletesPendingAck()
    {
        using var handler = new AckHandler(TimeSpan.FromSeconds(5));
        var ack = handler.CreateAck(1);

        handler.TriggerAck(1);

        await ack.WaitAsync(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task DisposeCancelsPendingAcks()
    {
        var handler = new AckHandler(TimeSpan.FromSeconds(5));
        var ack = handler.CreateAck(1);

        handler.Dispose();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => ack.WaitAsync(TimeSpan.FromSeconds(1)));
    }

    [Fact]
    public async Task TimedOutAckIsCanceled()
    {
        using var handler = new AckHandler(TimeSpan.FromMilliseconds(20));
        var ack = handler.CreateAck(1);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => ack.WaitAsync(TimeSpan.FromSeconds(2)));
    }
}
