namespace Kevsoft.AspNetCore.SignalR.MongoDB;

/// <summary>
/// Initializes MongoDB collections and indexes required by the SignalR backplane.
/// </summary>
/// <remarks>
/// Applications can resolve this service and run initialization during deployment or startup once an implementation is registered.
/// </remarks>
public interface IMongoDbSignalRCollectionInitializer
{
    /// <summary>
    /// Ensures the configured MongoDB collection and indexes exist.
    /// </summary>
    /// <param name="cancellationToken">A token that can cancel the operation.</param>
    ValueTask InitializeAsync(CancellationToken cancellationToken = default);
}
