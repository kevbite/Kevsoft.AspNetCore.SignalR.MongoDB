using Microsoft.AspNetCore.SignalR;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Tests;

internal sealed class TestInvocationBinder : IInvocationBinder
{
    public Type ReturnType { get; set; } = typeof(object);

    public IReadOnlyList<Type> ParameterTypes { get; set; } = [];

    public Type StreamItemType { get; set; } = typeof(object);

    public Type GetReturnType(string invocationId)
    {
        return ReturnType;
    }

    public IReadOnlyList<Type> GetParameterTypes(string methodName)
    {
        return ParameterTypes;
    }

    public Type GetStreamItemType(string streamId)
    {
        return StreamItemType;
    }
}
