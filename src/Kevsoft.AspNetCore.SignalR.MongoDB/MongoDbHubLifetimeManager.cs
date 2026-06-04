using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;
using Kevsoft.AspNetCore.SignalR.MongoDB.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Kevsoft.AspNetCore.SignalR.MongoDB;

/// <summary>
/// The MongoDB scale-out provider for ASP.NET Core SignalR.
/// </summary>
/// <typeparam name="THub">The type of <see cref="Hub"/> to manage connections for.</typeparam>
public class MongoDbHubLifetimeManager<THub> : HubLifetimeManager<THub>, IAsyncDisposable, IDisposable where THub : Hub
{
    private readonly HubConnectionStore _connections = new();
    private readonly MongoSubscriptionManager _groups = new();
    private readonly MongoSubscriptionManager _users = new();
    private readonly ConcurrentDictionary<string, IAsyncDisposable> _connectionSubscriptions = new(StringComparer.Ordinal);
    private readonly List<IAsyncDisposable> _coreSubscriptions = [];
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private readonly ILogger _logger;
    private readonly MongoDbSignalROptions _options;
    private readonly IMongoSignalRBackplane _backplane;
    private readonly MongoBackplaneChannels _channels;
    private readonly string _serverName = GenerateServerName();
    private readonly MongoDbBackplaneProtocol _protocol = new();
    private readonly AckHandler _ackHandler;
    private readonly ClientResultsManager _clientResultsManager = new();
    private readonly IHubProtocolResolver _hubProtocolResolver;
    private bool _backplaneStarted;
    private int _internalAckId;

    internal MongoDbHubLifetimeManager(
        ILogger<MongoDbHubLifetimeManager<THub>> logger,
        IOptions<MongoDbSignalROptions> options,
        IHubProtocolResolver hubProtocolResolver,
        IMongoSignalRBackplane backplane)
    {
        _logger = logger;
        _options = options.Value;
        _hubProtocolResolver = hubProtocolResolver;
        _backplane = backplane;
        _ackHandler = new AckHandler(_options.AckTimeout);
        _channels = new MongoBackplaneChannels(_options.ChannelPrefix ?? typeof(THub).FullName!, _serverName);
    }

    /// <inheritdoc />
    public override async Task OnConnectedAsync(HubConnectionContext connection)
    {
        await EnsureBackplaneStarted(connection.ConnectionAborted);

        connection.Features.Set<IMongoDbFeature>(new MongoDbFeature());
        _connections.Add(connection);

        var connectionTask = SubscribeToConnection(connection);
        var userTask = string.IsNullOrEmpty(connection.UserIdentifier)
            ? Task.CompletedTask
            : SubscribeToUser(connection);

        await Task.WhenAll(connectionTask, userTask);
    }

    /// <inheritdoc />
    public override Task OnDisconnectedAsync(HubConnectionContext connection)
    {
        _connections.Remove(connection);

        var tasks = new List<Task>();
        if (_connectionSubscriptions.TryRemove(connection.ConnectionId, out var subscription))
        {
            tasks.Add(subscription.DisposeAsync().AsTask());
        }

        if (connection.Features.Get<IMongoDbFeature>() is { } feature)
        {
            foreach (var groupName in feature.Groups.ToArray())
            {
                tasks.Add(RemoveGroupAsyncCore(connection, groupName));
            }
        }

        if (!string.IsNullOrEmpty(connection.UserIdentifier))
        {
            tasks.Add(RemoveUserAsync(connection));
        }

        return Task.WhenAll(tasks);
    }

    /// <inheritdoc />
    public override Task SendAllAsync(string methodName, object?[] args, CancellationToken cancellationToken = default)
    {
        return PublishAsync(_protocol.WriteInvocation(_channels.All, methodName, args, serverId: _serverName), cancellationToken);
    }

    /// <inheritdoc />
    public override Task SendAllExceptAsync(
        string methodName,
        object?[] args,
        IReadOnlyList<string> excludedConnectionIds,
        CancellationToken cancellationToken = default)
    {
        return PublishAsync(
            _protocol.WriteInvocation(_channels.All, methodName, args, excludedConnectionIds, serverId: _serverName),
            cancellationToken);
    }

    /// <inheritdoc />
    public override Task SendConnectionAsync(
        string connectionId,
        string methodName,
        object?[] args,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connectionId);

        var connection = _connections[connectionId];
        if (connection != null)
        {
            return connection.WriteAsync(new InvocationMessage(methodName, args), cancellationToken).AsTask();
        }

        return PublishAsync(
            _protocol.WriteInvocation(_channels.Connection(connectionId), methodName, args, serverId: _serverName),
            cancellationToken);
    }

    /// <inheritdoc />
    public override Task SendGroupAsync(
        string groupName,
        string methodName,
        object?[] args,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(groupName);

        return PublishAsync(
            _protocol.WriteInvocation(_channels.Group(groupName), methodName, args, serverId: _serverName),
            cancellationToken);
    }

    /// <inheritdoc />
    public override Task SendGroupExceptAsync(
        string groupName,
        string methodName,
        object?[] args,
        IReadOnlyList<string> excludedConnectionIds,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(groupName);

        return PublishAsync(
            _protocol.WriteInvocation(_channels.Group(groupName), methodName, args, excludedConnectionIds, serverId: _serverName),
            cancellationToken);
    }

    /// <inheritdoc />
    public override Task SendUserAsync(
        string userId,
        string methodName,
        object?[] args,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(userId);

        return PublishAsync(
            _protocol.WriteInvocation(_channels.User(userId), methodName, args, serverId: _serverName),
            cancellationToken);
    }

    /// <inheritdoc />
    public override Task SendConnectionsAsync(
        IReadOnlyList<string> connectionIds,
        string methodName,
        object?[] args,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connectionIds);

        var tasks = new List<Task>(connectionIds.Count);
        foreach (var connectionId in connectionIds)
        {
            tasks.Add(SendConnectionAsync(connectionId, methodName, args, cancellationToken));
        }

        return Task.WhenAll(tasks);
    }

    /// <inheritdoc />
    public override Task SendGroupsAsync(
        IReadOnlyList<string> groupNames,
        string methodName,
        object?[] args,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(groupNames);

        var tasks = new List<Task>(groupNames.Count);
        foreach (var groupName in groupNames)
        {
            if (!string.IsNullOrEmpty(groupName))
            {
                tasks.Add(SendGroupAsync(groupName, methodName, args, cancellationToken));
            }
        }

        return Task.WhenAll(tasks);
    }

    /// <inheritdoc />
    public override Task SendUsersAsync(
        IReadOnlyList<string> userIds,
        string methodName,
        object?[] args,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(userIds);

        var tasks = new List<Task>(userIds.Count);
        foreach (var userId in userIds)
        {
            if (!string.IsNullOrEmpty(userId))
            {
                tasks.Add(SendUserAsync(userId, methodName, args, cancellationToken));
            }
        }

        return Task.WhenAll(tasks);
    }

    /// <inheritdoc />
    public override Task AddToGroupAsync(string connectionId, string groupName, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connectionId);
        ArgumentNullException.ThrowIfNull(groupName);

        var connection = _connections[connectionId];
        return connection != null
            ? AddGroupAsyncCore(connection, groupName)
            : SendGroupActionAndWaitForAck(connectionId, groupName, GroupAction.Add, cancellationToken);
    }

    /// <inheritdoc />
    public override Task RemoveFromGroupAsync(string connectionId, string groupName, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connectionId);
        ArgumentNullException.ThrowIfNull(groupName);

        var connection = _connections[connectionId];
        return connection != null
            ? RemoveGroupAsyncCore(connection, groupName)
            : SendGroupActionAndWaitForAck(connectionId, groupName, GroupAction.Remove, cancellationToken);
    }

    /// <inheritdoc />
    public override async Task<T> InvokeConnectionAsync<T>(
        string connectionId,
        string methodName,
        object?[] args,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connectionId);

        var connection = _connections[connectionId];
        var invocationId = GenerateInvocationId();
        using var linkedTokenSource = connection is null
            ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
            : CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, connection.ConnectionAborted);

        var task = _clientResultsManager.AddInvocation<T>(connectionId, invocationId, linkedTokenSource.Token);

        try
        {
            if (connection == null)
            {
                await PublishAsync(
                    _protocol.WriteInvocation(
                        _channels.Connection(connectionId),
                        methodName,
                        args,
                        invocationId: invocationId,
                        returnChannel: _channels.ReturnResults,
                        serverId: _serverName),
                    cancellationToken);
            }
            else
            {
                await connection.WriteAsync(new InvocationMessage(invocationId, methodName, args), cancellationToken);
            }
        }
        catch
        {
            _clientResultsManager.RemoveInvocation(invocationId);
            throw;
        }

        try
        {
            return await task;
        }
        catch
        {
            if (connection?.ConnectionAborted.IsCancellationRequested == true)
            {
                throw new IOException($"Connection '{connectionId}' disconnected.");
            }

            throw;
        }
    }

    /// <inheritdoc />
    public override async Task SetConnectionResultAsync(string connectionId, CompletionMessage result)
    {
        await _clientResultsManager.TryCompleteResultAsync(connectionId, result);
    }

    /// <inheritdoc />
    public override bool TryGetReturnType(string invocationId, [NotNullWhen(true)] out Type? type)
    {
        return _clientResultsManager.TryGetType(invocationId, out type);
    }

    private async Task PublishAsync(MongoBackplaneEnvelope envelope, CancellationToken cancellationToken)
    {
        await EnsureBackplaneStarted(cancellationToken);
        await _backplane.PublishAsync(envelope, cancellationToken);
    }

    private Task AddGroupAsyncCore(HubConnectionContext connection, string groupName)
    {
        var feature = connection.Features.GetRequiredFeature<IMongoDbFeature>();
        lock (feature.Groups)
        {
            if (!feature.Groups.Add(groupName))
            {
                return Task.CompletedTask;
            }
        }

        return _groups.AddSubscriptionAsync(_channels.Group(groupName), connection, SubscribeToGroupAsync);
    }

    private async Task RemoveGroupAsyncCore(HubConnectionContext connection, string groupName)
    {
        await _groups.RemoveSubscriptionAsync(_channels.Group(groupName), connection);

        if (connection.Features.Get<IMongoDbFeature>() is { } feature)
        {
            lock (feature.Groups)
            {
                feature.Groups.Remove(groupName);
            }
        }
    }

    private async Task SendGroupActionAndWaitForAck(
        string connectionId,
        string groupName,
        GroupAction action,
        CancellationToken cancellationToken)
    {
        var id = Interlocked.Increment(ref _internalAckId);
        var ack = _ackHandler.CreateAck(id);
        var command = new MongoGroupCommand(id, _serverName, action, groupName, connectionId);

        await PublishAsync(
            MongoDbBackplaneProtocol.WriteGroupCommand(_channels.GroupManagement, command, _serverName),
            cancellationToken);

        await ack;
    }

    private Task SubscribeToUser(HubConnectionContext connection)
    {
        return _users.AddSubscriptionAsync(_channels.User(connection.UserIdentifier!), connection, SubscribeToUserAsync);
    }

    private Task RemoveUserAsync(HubConnectionContext connection)
    {
        return _users.RemoveSubscriptionAsync(_channels.User(connection.UserIdentifier!), connection);
    }

    private async Task EnsureBackplaneStarted(CancellationToken cancellationToken)
    {
        if (_backplaneStarted)
        {
            return;
        }

        await _connectionLock.WaitAsync(cancellationToken);
        try
        {
            if (_backplaneStarted)
            {
                return;
            }

            await _backplane.StartAsync(cancellationToken);
            _coreSubscriptions.Add(await SubscribeToAll());
            _coreSubscriptions.Add(await SubscribeToGroupManagementChannel());
            _coreSubscriptions.Add(await SubscribeToAckChannel());
            _coreSubscriptions.Add(await SubscribeToReturnResultsAsync());
            _backplaneStarted = true;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    private ValueTask<IAsyncDisposable> SubscribeToAll()
    {
        return _backplane.SubscribeAsync(_channels.All, async (envelope, _) =>
        {
            try
            {
                var invocation = _protocol.ReadInvocation(envelope);
                var tasks = new List<Task>(_connections.Count);
                foreach (var connection in _connections)
                {
                    if (invocation.ExcludedConnectionIds == null ||
                        !invocation.ExcludedConnectionIds.Contains(connection.ConnectionId))
                    {
                        tasks.Add(connection.WriteAsync(invocation.Message).AsTask());
                    }
                }

                await Task.WhenAll(tasks);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to write MongoDB SignalR all-channel message.");
            }
        });
    }

    private ValueTask<IAsyncDisposable> SubscribeToGroupManagementChannel()
    {
        return _backplane.SubscribeAsync(_channels.GroupManagement, async (envelope, cancellationToken) =>
        {
            try
            {
                var command = MongoDbBackplaneProtocol.ReadGroupCommand(envelope);
                var connection = _connections[command.ConnectionId];
                if (connection == null)
                {
                    return;
                }

                if (command.Action == GroupAction.Add)
                {
                    await AddGroupAsyncCore(connection, command.GroupName);
                }
                else if (command.Action == GroupAction.Remove)
                {
                    await RemoveGroupAsyncCore(connection, command.GroupName);
                }

                await PublishAsync(
                    MongoDbBackplaneProtocol.WriteAck(_channels.Ack(command.ServerName), command.Id, _serverName),
                    cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to process MongoDB SignalR group command.");
            }
        });
    }

    private ValueTask<IAsyncDisposable> SubscribeToAckChannel()
    {
        return _backplane.SubscribeAsync(_channels.Ack(_serverName), (envelope, _) =>
        {
            try
            {
                _ackHandler.TriggerAck(MongoDbBackplaneProtocol.ReadAck(envelope));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to process MongoDB SignalR acknowledgement.");
            }

            return ValueTask.CompletedTask;
        });
    }

    private async Task SubscribeToConnection(HubConnectionContext connection)
    {
        var subscription = await _backplane.SubscribeAsync(
            _channels.Connection(connection.ConnectionId),
            async (envelope, _) =>
            {
                try
                {
                    var invocation = _protocol.ReadInvocation(envelope);
                    if (!string.IsNullOrEmpty(invocation.InvocationId))
                    {
                        CancellationTokenRegistration? tokenRegistration = null;
                        _clientResultsManager.AddForwardingInvocation(
                            connection.ConnectionId,
                            invocation.InvocationId,
                            async completionMessage =>
                            {
                                tokenRegistration?.Dispose();
                                var bufferWriter = new ArrayBufferWriter<byte>();
                                connection.Protocol.WriteMessage(completionMessage, bufferWriter);
                                await PublishAsync(
                                    MongoDbBackplaneProtocol.WriteCompletion(
                                        invocation.ReturnChannel!,
                                        connection.Protocol.Name,
                                        bufferWriter.WrittenMemory,
                                        _serverName),
                                    CancellationToken.None);
                            });

                        tokenRegistration = connection.ConnectionAborted.Register(static state =>
                        {
                            var tuple = ((MongoDbHubLifetimeManager<THub> Manager, string InvocationId))state!;
                            var invocationInfo = tuple.Manager._clientResultsManager.RemoveInvocation(tuple.InvocationId);
                            var ignored = invocationInfo?.ForwardCompletion?.Invoke(
                                CompletionMessage.WithError(tuple.InvocationId, "Connection disconnected."));
                        }, (this, invocation.InvocationId));
                    }

                    await connection.WriteAsync(invocation.Message);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to write MongoDB SignalR connection-channel message.");
                }
            });

        _connectionSubscriptions[connection.ConnectionId] = subscription;
    }

    private ValueTask<IAsyncDisposable> SubscribeToUserAsync(string userChannel, HubConnectionStore subscriptions)
    {
        return _backplane.SubscribeAsync(userChannel, async (envelope, _) =>
        {
            try
            {
                var invocation = _protocol.ReadInvocation(envelope);
                var tasks = new List<Task>(subscriptions.Count);
                foreach (var connection in subscriptions)
                {
                    tasks.Add(connection.WriteAsync(invocation.Message).AsTask());
                }

                await Task.WhenAll(tasks);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to write MongoDB SignalR user-channel message.");
            }
        });
    }

    private ValueTask<IAsyncDisposable> SubscribeToGroupAsync(string groupChannel, HubConnectionStore subscriptions)
    {
        return _backplane.SubscribeAsync(groupChannel, async (envelope, _) =>
        {
            try
            {
                var invocation = _protocol.ReadInvocation(envelope);
                var tasks = new List<Task>(subscriptions.Count);
                foreach (var connection in subscriptions)
                {
                    if (invocation.ExcludedConnectionIds?.Contains(connection.ConnectionId) == true)
                    {
                        continue;
                    }

                    tasks.Add(connection.WriteAsync(invocation.Message).AsTask());
                }

                await Task.WhenAll(tasks);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to write MongoDB SignalR group-channel message.");
            }
        });
    }

    private ValueTask<IAsyncDisposable> SubscribeToReturnResultsAsync()
    {
        return _backplane.SubscribeAsync(_channels.ReturnResults, async (envelope, cancellationToken) =>
        {
            try
            {
                var completion = MongoDbBackplaneProtocol.ReadCompletion(envelope);
                var protocol = _hubProtocolResolver.AllProtocols.FirstOrDefault(protocol =>
                    string.Equals(protocol.Name, completion.ProtocolName, StringComparison.Ordinal));

                if (protocol == null)
                {
                    _logger.LogError(
                        "Unable to process MongoDB SignalR client result because protocol '{Protocol}' is unavailable.",
                        completion.ProtocolName);
                    return;
                }

                if (TryParseCompletion(protocol, completion.CompletionMessage, _clientResultsManager, out var completionMessage))
                {
                    await _clientResultsManager.TryCompleteResultAsync(completionMessage);
                    return;
                }

                if (TryParseCompletion(protocol, completion.CompletionMessage, RawResultInvocationBinder.Instance, out completionMessage))
                {
                    var invocationId = completionMessage.InvocationId!;
                    var expectedType = _clientResultsManager.TryGetType(invocationId, out var type)
                        ? type!.FullName ?? type.Name
                        : "the expected type";

                    await _clientResultsManager.TryCompleteResultAsync(
                        CompletionMessage.WithError(
                            invocationId,
                            $"Client result could not be deserialized as {expectedType}."));

                    _logger.LogError(
                        "Unable to deserialize MongoDB SignalR client result '{InvocationId}' as {ExpectedType}.",
                        invocationId,
                        expectedType);
                    return;
                }

                _logger.LogError("Unable to parse MongoDB SignalR client result for protocol '{Protocol}'.", completion.ProtocolName);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to process MongoDB SignalR client result.");
            }
        });
    }

    private static bool TryParseCompletion(
        IHubProtocol protocol,
        ReadOnlyMemory<byte> completionMessage,
        IInvocationBinder binder,
        [NotNullWhen(true)] out CompletionMessage? completion)
    {
        var sequence = new ReadOnlySequence<byte>(completionMessage);
        completion = null;

        try
        {
            if (protocol.TryParseMessage(ref sequence, binder, out var hubMessage) &&
                hubMessage is CompletionMessage parsedCompletion &&
                !string.IsNullOrEmpty(parsedCompletion.InvocationId))
            {
                completion = parsedCompletion;
                return true;
            }
        }
        catch (InvalidDataException)
        {
        }

        return false;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        _ackHandler.Dispose();
        _clientResultsManager.Dispose();
        _connectionLock.Dispose();

        foreach (var subscription in _connectionSubscriptions.Values)
        {
            await subscription.DisposeAsync();
        }

        foreach (var subscription in _coreSubscriptions)
        {
            await subscription.DisposeAsync();
        }

        await _groups.DisposeAsync();
        await _users.DisposeAsync();
        await _backplane.DisposeAsync();
    }

    private static string GenerateServerName()
    {
        return $"{Environment.MachineName}_{Guid.NewGuid():N}";
    }

    private static string GenerateInvocationId()
    {
        Span<byte> buffer = stackalloc byte[16];
        var success = Guid.NewGuid().TryWriteBytes(buffer);
        Debug.Assert(success);

        Span<char> base64 = stackalloc char[24];
        success = Convert.TryToBase64Chars(buffer, base64, out var written);
        Debug.Assert(success);
        Debug.Assert(written == 24);

        return new string(base64[..^2]);
    }

    private interface IMongoDbFeature
    {
        HashSet<string> Groups { get; }
    }

    private sealed class MongoDbFeature : IMongoDbFeature
    {
        public HashSet<string> Groups { get; } = new(StringComparer.OrdinalIgnoreCase);
    }
}
