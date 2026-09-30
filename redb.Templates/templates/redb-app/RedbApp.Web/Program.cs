using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.Components.WebAssembly.Hosting;
using RedbApp.Web;
using RedbApp.Web.Services;

var builder = WebAssemblyHostBuilder.CreateDefault(args);
builder.RootComponents.Add<App>("#app");
builder.RootComponents.Add<HeadOutlet>("head::after");

// Where the API is. wwwroot/appsettings.Development.json points at the API host on its own port; in a
// deployment the setting is empty and the client calls its own origin, where nginx forwards /api/.
var apiBase = builder.Configuration["ApiBaseUrl"] is { Length: > 0 } configured
    ? configured
    : builder.HostEnvironment.BaseAddress;

builder.Services.AddSingleton<Session>();
builder.Services.AddScoped(_ => new HttpClient { BaseAddress = new Uri(apiBase) });
builder.Services.AddScoped<ApiClient>();

await builder.Build().RunAsync();
