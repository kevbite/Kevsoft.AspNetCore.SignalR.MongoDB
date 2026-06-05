using System.Buffers;
using System.IO.Pipelines;
using System.Security.Claims;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Connections.Features;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.IntegrationTests;

internal sealed class MongoTestClient : ITransferFormatFeature, IDisposable
{
    private readonly IHubProtocol _protocol = new JsonHubProtocol();
    private readonly TestInvocationBinder _binder = new();

    public MongoTestClient()
    {
        var options = new PipeOptions(
            readerScheduler: PipeScheduler.Inline,
            writerScheduler: PipeScheduler.Inline,
            useSynchronizationContext: false);
        var pair = DuplexPipe.CreateConnectionPair(options, options);

        Connection = new DefaultConnectionContext(Guid.NewGuid().ToString("N"), pair.Transport, pair.Application);
        Connection.Features.Set<ITransferFormatFeature>(this);
    }

    public DefaultConnectionContext Connection { get; }

    public TransferFormat SupportedFormats { get; set; } = TransferFormat.Text | TransferFormat.Binary;

    public TransferFormat ActiveFormat { get; set; }

    public HubConnectionContext CreateHubConnectionContext(string? userId = null)
    {
        if (userId is not null)
        {
            Connection.User = new ClaimsPrincipal(
                new ClaimsIdentity(
                    [new Claim(ClaimTypes.NameIdentifier, userId)]));
        }

        var ctx = new HubConnectionContext(
            Connection,
            new HubConnectionContextOptions
            {
                KeepAliveInterval = TimeSpan.FromSeconds(15)
            },
            NullLoggerFactory.Instance)
        {
            Protocol = _protocol
        };

        if (userId is not null)
        {
            // HubConnectionContext.UserIdentifier has internal set in ASP.NET Core; use reflection
            // to wire it up so the manager registers the correct user subscriptions.
            typeof(HubConnectionContext)
                .GetProperty("UserIdentifier")!
                .SetValue(ctx, userId);
        }

        return ctx;
    }

    public async Task<HubMessage> ReadAsync(CancellationToken cancellationToken = default)
    {
        while (true)
        {
            var message = TryRead();
            if (message != null)
            {
                return message;
            }

            var result = await Connection.Application!.Input.ReadAsync(cancellationToken);
            var buffer = result.Buffer;

            try
            {
                if (buffer.IsEmpty && result.IsCompleted)
                {
                    throw new EndOfStreamException("The test client connection completed before a hub message was available.");
                }
            }
            finally
            {
                Connection.Application!.Input.AdvanceTo(buffer.Start);
            }
        }
    }

    public async Task SendAsync(HubMessage message, CancellationToken cancellationToken = default)
    {
        await Connection.Application!.Output.WriteAsync(_protocol.GetMessageBytes(message), cancellationToken);
    }

    public void Dispose()
    {
        Connection.Application!.Input.Complete();
        Connection.Application.Output.Complete();
    }

    /// <summary>
    /// Returns true if there is at least one buffered message available to read without consuming it.
    /// </summary>
    public bool HasPendingMessage()
    {
        if (!Connection.Application!.Input.TryRead(out var result))
        {
            return false;
        }

        var buffer = result.Buffer;
        // Peek: examined but not consumed — the data remains available for future reads.
        Connection.Application.Input.AdvanceTo(buffer.Start, buffer.End);
        return !buffer.IsEmpty;
    }

    private HubMessage? TryRead()
    {
        if (!Connection.Application!.Input.TryRead(out var result))
        {
            return null;
        }

        var buffer = result.Buffer;
        try
        {
            return _protocol.TryParseMessage(ref buffer, _binder, out var message) ? message : null;
        }
        finally
        {
            Connection.Application!.Input.AdvanceTo(buffer.Start);
        }
    }

    private sealed class TestInvocationBinder : IInvocationBinder
    {
        public Type GetReturnType(string invocationId)
        {
            return typeof(object);
        }

        public IReadOnlyList<Type> GetParameterTypes(string methodName)
        {
            return [typeof(string)];
        }

        public Type GetStreamItemType(string streamId)
        {
            return typeof(object);
        }
    }

    private sealed class DuplexPipe(PipeReader reader, PipeWriter writer) : IDuplexPipe
    {
        public PipeReader Input { get; } = reader;

        public PipeWriter Output { get; } = writer;

        public static DuplexPipePair CreateConnectionPair(PipeOptions inputOptions, PipeOptions outputOptions)
        {
            var input = new Pipe(inputOptions);
            var output = new Pipe(outputOptions);

            var transportToApplication = new DuplexPipe(output.Reader, input.Writer);
            var applicationToTransport = new DuplexPipe(input.Reader, output.Writer);
            return new DuplexPipePair(applicationToTransport, transportToApplication);
        }
    }

    private readonly struct DuplexPipePair(IDuplexPipe transport, IDuplexPipe application)
    {
        public IDuplexPipe Transport { get; } = transport;

        public IDuplexPipe Application { get; } = application;
    }
}
