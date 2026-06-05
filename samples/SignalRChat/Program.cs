using SignalRChat.Hubs;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddRazorPages();
builder.Services
    .AddSignalR()
    .AddMongoDb(options =>
    {
        options.UseConnectionString(
            builder.Configuration.GetConnectionString("MongoDB")!,
            "signalr_chat");
        options.UseChangeStreams();
        options.CollectionName = "signalr_messages";
        options.ChannelPrefix = "chat";
    });

var app = builder.Build();

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error");
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseRouting();
app.UseAuthorization();

app.MapStaticAssets();
app.MapHub<ChatHub>("/chatHub");
app.MapRazorPages()
   .WithStaticAssets();

app.Run();
