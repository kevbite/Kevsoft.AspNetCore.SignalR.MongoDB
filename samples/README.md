# SignalR Chat Sample

A simple real-time chat application based on the [ASP.NET Core SignalR tutorial](https://learn.microsoft.com/en-us/aspnet/core/tutorials/signalr?view=aspnetcore-10.0), extended to demonstrate scale-out using `Kevsoft.AspNetCore.SignalR.MongoDB` with the Change Streams transport.

## Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0)
- [Docker](https://www.docker.com/get-started) (for MongoDB)

## Start MongoDB

From the `samples/` directory, start a single-node MongoDB replica set (required for Change Streams):

```bash
docker compose up -d
```

This starts MongoDB on port `27017` and automatically initialises the `rs0` replica set.

## Run the app
https://bitbucket.org/exizent_team/frontend-monorepo/pull-requests/1809/
From the `samples/SignalRChat/` directory:

```bash
dotnet run
```

Open `http://localhost:5001` in a browser, enter a username and start chatting.

## Verify scale-out (run two instances)

Open two terminal windows and run both profiles to observe that messages sent from one instance are received by clients connected to the other:

```bash
# Terminal 1
dotnet run --launch-profile https

# Terminal 2
dotnet run --launch-profile https-instance2
```

Open `https://localhost:7001` in one browser tab and `https://localhost:7002` in another. Messages sent in either tab should appear in both — routed through the MongoDB backplane.

## How it works

```
Browser A ──► Instance 1 ──► MongoDB (Change Stream) ──► Instance 2 ──► Browser B
                           ◄─────────────────────────────
```

- Each app instance subscribes to the `signalr_messages` collection via a MongoDB change stream.
- When a client sends a message, the hub inserts a backplane document into MongoDB.
- All instances (including the publisher) receive the change event and forward the message to their local SignalR clients.

## Configuration

Connection settings live in `appsettings.json`:

```json
{
  "ConnectionStrings": {
    "MongoDB": "mongodb://localhost:27017"
  }
}
```

The database (`signalr_chat`), collection (`signalr_messages`), and channel prefix (`chat`) are set in `Program.cs`.
