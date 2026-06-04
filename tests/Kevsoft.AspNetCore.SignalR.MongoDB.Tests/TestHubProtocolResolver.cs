using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Tests;

internal sealed class TestHubProtocolResolver(params IHubProtocol[] protocols) : IHubProtocolResolver
{
    private readonly IReadOnlyList<IHubProtocol> _protocols = protocols;

    public IReadOnlyList<IHubProtocol> AllProtocols => _protocols;

    public IHubProtocol? GetProtocol(string protocolName, IReadOnlyList<string>? supportedProtocols)
    {
        return _protocols.FirstOrDefault(protocol => string.Equals(protocol.Name, protocolName, StringComparison.Ordinal));
    }
}
