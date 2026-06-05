using MongoDB.Bson.Serialization.Attributes;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

/// <summary>
/// Discriminated base for all backplane payload types. Each subtype carries a stable
/// <c>TypeDiscriminator</c> constant that is written as <c>_t</c> inside the BSON
/// payload subdocument, giving both a typed C# hierarchy and a stable wire format.
/// </summary>
[BsonDiscriminator(RootClass = true)]
[BsonKnownTypes(
    typeof(MongoInvocationPayload),
    typeof(MongoGroupCommandPayload),
    typeof(MongoAckPayload),
    typeof(MongoCompletionPayload))]
internal abstract record MongoBackplanePayload;

[BsonDiscriminator(MongoInvocationPayload.TypeDiscriminator)]
internal sealed record MongoInvocationPayload(MongoInvocation Invocation) : MongoBackplanePayload
{
    public const string TypeDiscriminator = "invocation";
}

[BsonDiscriminator(MongoGroupCommandPayload.TypeDiscriminator)]
internal sealed record MongoGroupCommandPayload(MongoGroupCommand Command) : MongoBackplanePayload
{
    public const string TypeDiscriminator = "group_command";
}

[BsonDiscriminator(MongoAckPayload.TypeDiscriminator)]
internal sealed record MongoAckPayload(int Id) : MongoBackplanePayload
{
    public const string TypeDiscriminator = "ack";
}

[BsonDiscriminator(MongoCompletionPayload.TypeDiscriminator)]
internal sealed record MongoCompletionPayload(MongoCompletion Completion) : MongoBackplanePayload
{
    public const string TypeDiscriminator = "completion";
}
