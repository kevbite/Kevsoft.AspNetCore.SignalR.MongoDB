using Kevsoft.AspNetCore.SignalR.MongoDB.Internal;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Tests;

public class ClientResultsManagerTests
{
    [Fact]
    public async Task AddInvocationCompletesWithTypedResult()
    {
        using var manager = new ClientResultsManager();
        var resultTask = manager.AddInvocation<int>("connection", "invocation", CancellationToken.None);

        await manager.TryCompleteResultAsync("connection", CompletionMessage.WithResult("invocation", 42));

        Assert.Equal(42, await resultTask);
    }

    [Fact]
    public void ForwardingInvocationUsesRawResultReturnType()
    {
        using var manager = new ClientResultsManager();

        manager.AddForwardingInvocation("connection", "invocation", _ => ValueTask.CompletedTask);

        Assert.True(manager.TryGetType("invocation", out var type));
        Assert.Equal(typeof(RawResult), type);
    }

    [Fact]
    public async Task CompletionErrorFailsTypedInvocation()
    {
        using var manager = new ClientResultsManager();
        var resultTask = manager.AddInvocation<int>("connection", "invocation", CancellationToken.None);

        await manager.TryCompleteResultAsync(
            "connection",
            CompletionMessage.WithError("invocation", "Client result could not be deserialized."));

        var exception = await Assert.ThrowsAsync<HubException>(() => resultTask);
        Assert.Equal("Client result could not be deserialized.", exception.Message);
    }

    [Fact]
    public async Task AddDuplicateInvocationIdThrows()
    {
        using var manager = new ClientResultsManager();
        _ = manager.AddInvocation<int>("connection", "invocation", CancellationToken.None);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => manager.AddInvocation<int>("connection", "invocation", CancellationToken.None));
    }

    [Fact]
    public void RemoveInvocationReturnsNullForUnknownId()
    {
        using var manager = new ClientResultsManager();

        var result = manager.RemoveInvocation("nonexistent");

        Assert.Null(result);
    }

    [Fact]
    public async Task DisposeWhilePendingInvocationCancelsIt()
    {
        var manager = new ClientResultsManager();
        var resultTask = manager.AddInvocation<int>("connection", "invocation", CancellationToken.None);

        manager.Dispose();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => resultTask);
    }

    [Fact]
    public async Task CancellationTokenCancelsInvocationAndRemovesIt()
    {
        using var manager = new ClientResultsManager();
        using var cts = new CancellationTokenSource();
        var resultTask = manager.AddInvocation<int>("connection", "invocation", cts.Token);

        cts.Cancel();

        await Assert.ThrowsAnyAsync<Exception>(() => resultTask);
        // Invocation should have been cleaned up.
        Assert.Null(manager.RemoveInvocation("invocation"));
    }

    [Fact]
    public async Task TryCompleteResultReturnsFalseForUnknownInvocationId()
    {
        using var manager = new ClientResultsManager();

        var completed = await manager.TryCompleteResultAsync(
            CompletionMessage.WithResult("nonexistent", 1));

        Assert.False(completed);
    }
}
