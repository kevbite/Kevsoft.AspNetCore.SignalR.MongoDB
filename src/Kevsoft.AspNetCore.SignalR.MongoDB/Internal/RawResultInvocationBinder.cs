using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal sealed class RawResultInvocationBinder : IInvocationBinder
{
    public static RawResultInvocationBinder Instance { get; } = new();

    private RawResultInvocationBinder()
    {
    }

    public Type GetReturnType(string invocationId)
    {
        return typeof(RawResult);
    }

    public IReadOnlyList<Type> GetParameterTypes(string methodName)
    {
        throw new NotSupportedException("Client result parsing does not provide invocation parameter types.");
    }

    public Type GetStreamItemType(string streamId)
    {
        throw new NotSupportedException("Client result parsing does not provide stream item types.");
    }
}
