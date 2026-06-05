using Microsoft.AspNetCore.SignalR;

namespace SignalRChat.Hubs;

public class ChatHub : Hub
{
    public async Task SendMessage(string user, string message)
    {
        await Clients.All.SendAsync("ReceiveMessage", user, message, null);
    }

    public async Task SendMessageToGroup(string user, string group, string message)
    {
        await Clients.Group(group).SendAsync("ReceiveMessage", user, message, group);
    }

    public async Task JoinGroup(string group)
    {
        await Groups.AddToGroupAsync(Context.ConnectionId, group);
        await Clients.Caller.SendAsync("JoinedGroup", group);
    }

    public async Task LeaveGroup(string group)
    {
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, group);
        await Clients.Caller.SendAsync("LeftGroup", group);
    }
}
