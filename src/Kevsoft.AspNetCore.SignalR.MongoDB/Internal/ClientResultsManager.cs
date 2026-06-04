using System.Collections.Concurrent;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal sealed class ClientResultsManager : IInvocationBinder, IDisposable
{
    private readonly ConcurrentDictionary<string, InvocationInfo> _invocations = new(StringComparer.Ordinal);

    public Task<T> AddInvocation<T>(string connectionId, string invocationId, CancellationToken cancellationToken)
    {
        var tcs = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        CancellationTokenRegistration? registration = null;

        if (cancellationToken.CanBeCanceled)
        {
            registration = cancellationToken.Register(static state =>
            {
                var tuple = ((ClientResultsManager Manager, string InvocationId))state!;
                if (tuple.Manager._invocations.TryRemove(tuple.InvocationId, out var invocation))
                {
                    invocation.TrySetCanceled();
                }
            }, (this, invocationId));
        }

        var info = InvocationInfo.ForResult(connectionId, typeof(T), tcs, registration);
        if (!_invocations.TryAdd(invocationId, info))
        {
            registration?.Dispose();
            throw new InvalidOperationException($"Invocation '{invocationId}' is already registered.");
        }

        return AwaitResult<T>(invocationId, tcs.Task);
    }

    public void AddForwardingInvocation(
        string connectionId,
        string invocationId,
        Func<CompletionMessage, ValueTask> forwardCompletion)
    {
        var info = InvocationInfo.ForForwarding(connectionId, forwardCompletion);
        if (!_invocations.TryAdd(invocationId, info))
        {
            throw new InvalidOperationException($"Invocation '{invocationId}' is already registered.");
        }
    }

    public InvocationInfo? RemoveInvocation(string invocationId)
    {
        if (_invocations.TryRemove(invocationId, out var invocation))
        {
            invocation.Dispose();
            return invocation;
        }

        return null;
    }

    public async Task<bool> TryCompleteResultAsync(string connectionId, CompletionMessage completionMessage)
    {
        if (string.IsNullOrEmpty(completionMessage.InvocationId) ||
            !_invocations.TryGetValue(completionMessage.InvocationId, out var invocation) ||
            !string.Equals(invocation.ConnectionId, connectionId, StringComparison.Ordinal))
        {
            return false;
        }

        if (invocation.ForwardCompletion != null)
        {
            await invocation.ForwardCompletion(completionMessage);
            RemoveInvocation(completionMessage.InvocationId);
            return true;
        }

        RemoveInvocation(completionMessage.InvocationId);
        invocation.Complete(completionMessage);
        return true;
    }

    public Task<bool> TryCompleteResultAsync(CompletionMessage completionMessage)
    {
        if (string.IsNullOrEmpty(completionMessage.InvocationId) ||
            !_invocations.TryGetValue(completionMessage.InvocationId, out var invocation))
        {
            return Task.FromResult(false);
        }

        return TryCompleteResultAsync(invocation.ConnectionId, completionMessage);
    }

    public bool TryGetType(string invocationId, out Type? type)
    {
        if (_invocations.TryGetValue(invocationId, out var invocation))
        {
            type = invocation.ResultType;
            return true;
        }

        type = null;
        return false;
    }

    public Type GetReturnType(string invocationId)
    {
        if (TryGetType(invocationId, out var type))
        {
            return type!;
        }

        throw new InvalidOperationException($"Invocation '{invocationId}' is not registered.");
    }

    public IReadOnlyList<Type> GetParameterTypes(string methodName)
    {
        throw new NotSupportedException("Client result parsing does not provide invocation parameter types.");
    }

    public Type GetStreamItemType(string streamId)
    {
        throw new NotSupportedException("Client result parsing does not provide stream item types.");
    }

    public void Dispose()
    {
        foreach (var pair in _invocations)
        {
            if (_invocations.TryRemove(pair.Key, out var invocation))
            {
                invocation.TrySetCanceled();
                invocation.Dispose();
            }
        }
    }

    private static async Task<T> AwaitResult<T>(string invocationId, Task<object?> task)
    {
        var result = await task;
        if (result is null)
        {
            return default!;
        }

        if (result is T typed)
        {
            return typed;
        }

        throw new InvalidCastException($"Client result for invocation '{invocationId}' could not be cast to '{typeof(T)}'.");
    }

    internal sealed class InvocationInfo : IDisposable
    {
        private readonly TaskCompletionSource<object?>? _taskCompletionSource;
        private readonly CancellationTokenRegistration? _cancellationTokenRegistration;

        private InvocationInfo(
            string connectionId,
            Type resultType,
            TaskCompletionSource<object?>? taskCompletionSource,
            CancellationTokenRegistration? cancellationTokenRegistration,
            Func<CompletionMessage, ValueTask>? forwardCompletion)
        {
            ConnectionId = connectionId;
            ResultType = resultType;
            _taskCompletionSource = taskCompletionSource;
            _cancellationTokenRegistration = cancellationTokenRegistration;
            ForwardCompletion = forwardCompletion;
        }

        public string ConnectionId { get; }

        public Type ResultType { get; }

        public Func<CompletionMessage, ValueTask>? ForwardCompletion { get; }

        public static InvocationInfo ForResult(
            string connectionId,
            Type resultType,
            TaskCompletionSource<object?> taskCompletionSource,
            CancellationTokenRegistration? cancellationTokenRegistration)
        {
            return new InvocationInfo(connectionId, resultType, taskCompletionSource, cancellationTokenRegistration, null);
        }

        public static InvocationInfo ForForwarding(
            string connectionId,
            Func<CompletionMessage, ValueTask> forwardCompletion)
        {
            return new InvocationInfo(connectionId, typeof(RawResult), null, null, forwardCompletion);
        }

        public void Complete(CompletionMessage completionMessage)
        {
            if (_taskCompletionSource is null)
            {
                return;
            }

            if (!string.IsNullOrEmpty(completionMessage.Error))
            {
                _taskCompletionSource.TrySetException(new HubException(completionMessage.Error));
                return;
            }

            _taskCompletionSource.TrySetResult(completionMessage.HasResult ? completionMessage.Result : null);
        }

        public void TrySetCanceled()
        {
            _taskCompletionSource?.TrySetCanceled();
        }

        public void Dispose()
        {
            _cancellationTokenRegistration?.Dispose();
        }
    }
}
