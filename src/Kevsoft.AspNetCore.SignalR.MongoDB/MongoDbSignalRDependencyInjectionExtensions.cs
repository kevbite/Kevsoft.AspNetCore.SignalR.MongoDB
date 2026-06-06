using Kevsoft.AspNetCore.SignalR.MongoDB;
using Kevsoft.AspNetCore.SignalR.MongoDB.Internal;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Extension methods for configuring MongoDB-based scale-out for SignalR.
/// </summary>
public static class MongoDbSignalRDependencyInjectionExtensions
{
    /// <summary>
    /// Adds MongoDB scale-out to a SignalR server.
    /// </summary>
    /// <param name="signalrBuilder">The SignalR server builder.</param>
    /// <returns>The same <see cref="ISignalRServerBuilder"/> instance for chaining.</returns>
    public static ISignalRServerBuilder AddMongoDb(this ISignalRServerBuilder signalrBuilder)
    {
        return AddMongoDb(signalrBuilder, static _ => { });
    }

    /// <summary>
    /// Adds MongoDB scale-out to a SignalR server.
    /// </summary>
    /// <param name="signalrBuilder">The SignalR server builder.</param>
    /// <param name="connectionString">The MongoDB connection string.</param>
    /// <returns>The same <see cref="ISignalRServerBuilder"/> instance for chaining.</returns>
    public static ISignalRServerBuilder AddMongoDb(this ISignalRServerBuilder signalrBuilder, string connectionString)
    {
        return AddMongoDb(signalrBuilder, connectionString, static _ => { });
    }

    /// <summary>
    /// Adds MongoDB scale-out to a SignalR server.
    /// </summary>
    /// <param name="signalrBuilder">The SignalR server builder.</param>
    /// <param name="configure">A callback used to configure MongoDB scale-out options.</param>
    /// <returns>The same <see cref="ISignalRServerBuilder"/> instance for chaining.</returns>
    public static ISignalRServerBuilder AddMongoDb(
        this ISignalRServerBuilder signalrBuilder,
        Action<MongoDbSignalROptions> configure)
    {
        ArgumentNullException.ThrowIfNull(signalrBuilder);
        ArgumentNullException.ThrowIfNull(configure);

        signalrBuilder.Services.Configure(configure);
        AddMongoDbServices(signalrBuilder.Services);

        return signalrBuilder;
    }

    /// <summary>
    /// Adds MongoDB scale-out to a SignalR server.
    /// </summary>
    /// <param name="signalrBuilder">The SignalR server builder.</param>
    /// <param name="connectionString">The MongoDB connection string.</param>
    /// <param name="configure">A callback used to configure MongoDB scale-out options.</param>
    /// <returns>The same <see cref="ISignalRServerBuilder"/> instance for chaining.</returns>
    public static ISignalRServerBuilder AddMongoDb(
        this ISignalRServerBuilder signalrBuilder,
        string connectionString,
        Action<MongoDbSignalROptions> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentNullException.ThrowIfNull(configure);

        return AddMongoDb(signalrBuilder, options =>
        {
            options.UseConnectionString(connectionString);
            configure(options);
        });
    }

    /// <summary>
    /// Adds MongoDB scale-out to a SignalR server.
    /// </summary>
    /// <param name="signalrBuilder">The SignalR server builder.</param>
    /// <param name="configure">
    /// A callback used to configure MongoDB scale-out options. The <see cref="IServiceProvider"/>
    /// passed to the callback is the root (singleton-scope) service provider; do not resolve
    /// scoped services from it.
    /// </param>
    /// <returns>The same <see cref="ISignalRServerBuilder"/> instance for chaining.</returns>
    public static ISignalRServerBuilder AddMongoDb(
        this ISignalRServerBuilder signalrBuilder,
        Action<IServiceProvider, MongoDbSignalROptions> configure)
    {
        ArgumentNullException.ThrowIfNull(signalrBuilder);
        ArgumentNullException.ThrowIfNull(configure);

        signalrBuilder.Services.AddSingleton<IConfigureOptions<MongoDbSignalROptions>>(sp =>
            new ConfigureNamedOptions<MongoDbSignalROptions>(
                Microsoft.Extensions.Options.Options.DefaultName,
                opts => configure(sp, opts)));
        AddMongoDbServices(signalrBuilder.Services);

        return signalrBuilder;
    }

    /// <summary>
    /// Adds MongoDB scale-out to a SignalR server.
    /// </summary>
    /// <param name="signalrBuilder">The SignalR server builder.</param>
    /// <param name="connectionString">The MongoDB connection string.</param>
    /// <param name="configure">
    /// A callback used to configure MongoDB scale-out options. The <see cref="IServiceProvider"/>
    /// passed to the callback is the root (singleton-scope) service provider; do not resolve
    /// scoped services from it.
    /// </param>
    /// <returns>The same <see cref="ISignalRServerBuilder"/> instance for chaining.</returns>
    public static ISignalRServerBuilder AddMongoDb(
        this ISignalRServerBuilder signalrBuilder,
        string connectionString,
        Action<IServiceProvider, MongoDbSignalROptions> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentNullException.ThrowIfNull(configure);

        return AddMongoDb(signalrBuilder, (sp, options) =>
        {
            options.UseConnectionString(connectionString);
            configure(sp, options);
        });
    }

    private static void AddMongoDbServices(IServiceCollection services)
    {
        services.TryAddSingleton<IMessageCheckpointStore, InMemoryMessageCheckpointStore>();
        services.TryAddSingleton<IBackplaneEnvelopeSerializer, BsonBackplaneEnvelopeSerializer>();

        services.AddTransient(MongoDbSignalRServiceFactory.CreateBackplane);
        services.AddTransient(static serviceProvider =>
            (IMongoDbSignalRCollectionInitializer)MongoDbSignalRServiceFactory.CreateBackplane(serviceProvider));
        services.AddSingleton(typeof(HubLifetimeManager<>), typeof(MongoDbHubLifetimeManager<>));
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<MongoDbSignalROptions>, MongoDbSignalROptionsValidator>());

        var optionsBuilder = services.AddOptions<MongoDbSignalROptions>();
        optionsBuilder.Configure<IMessageCheckpointStore>(static (options, checkpointStore) =>
        {
            options.CheckpointStore ??= checkpointStore;
        });
        optionsBuilder.ValidateOnStart();
    }
}
