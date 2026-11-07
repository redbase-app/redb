using System.Net.Http.Headers;
using System.Security.Claims;
using Microsoft.AspNetCore.Antiforgery;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using RedbBff.Web.Components;
using RedbBff.Web.Services;

var builder = WebApplication.CreateBuilder(args);

// --- Session: an ASP.NET Core auth cookie ---
// The browser holds only the cookie; the service key to the backend stays on this server. Pages are
// protected by the [Authorize] attribute in Components/_Imports.razor.
builder.Services.AddAuthentication(CookieAuthenticationDefaults.AuthenticationScheme)
    .AddCookie(options =>
    {
        options.Cookie.HttpOnly = true;
        options.Cookie.SameSite = SameSiteMode.Strict;
        // Secure when the request is HTTPS. Behind a TLS-terminating proxy, add UseForwardedHeaders so the
        // request scheme is the real one.
        options.Cookie.SecurePolicy = CookieSecurePolicy.SameAsRequest;
        options.LoginPath = "/login";
        options.ExpireTimeSpan = TimeSpan.FromHours(8);
        options.SlidingExpiration = true;
    });
builder.Services.AddAuthorization();
builder.Services.AddCascadingAuthenticationState();

builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

builder.Services.AddSingleton<WebUsers>();

// --- The backend ---
var backendUrl = builder.Configuration["Backend:BaseUrl"]
    ?? throw new InvalidOperationException("Backend:BaseUrl is missing in appsettings.json.");
var serviceKey = builder.Configuration["Backend:ServiceKey"]
    ?? throw new InvalidOperationException("Backend:ServiceKey is missing in appsettings.json.");
builder.Services.AddHttpClient<BackendClient>(http =>
{
    http.BaseAddress = new Uri(backendUrl);
    http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", serviceKey);
});

var app = builder.Build();

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
}

app.UseAuthentication();
app.UseAuthorization();
app.UseAntiforgery();
app.MapStaticAssets();

// --- Sign-in and sign-out ---
// SignInAsync writes the Set-Cookie header, which only a plain HTTP request can do: an interactive
// circuit has started its response long ago. So the sign-in form of Login.razor POSTs here. These two
// endpoints belong to the web server's session; the data goes through the backend.
app.MapPost("/auth/login", async (HttpContext http, IAntiforgery antiforgery, WebUsers users) =>
{
    // Absolute redirects: a relative "login" would resolve under /auth/.
    if (!await antiforgery.IsRequestValidAsync(http))
        return Results.Redirect("/login?error=stale");

    var form = await http.Request.ReadFormAsync();
    var login = form["login"].ToString().Trim();
    var role = users.Check(login, form["password"].ToString());
    if (role is null)
        return Results.Redirect("/login?error=1");

    var identity = new ClaimsIdentity(CookieAuthenticationDefaults.AuthenticationScheme, ClaimTypes.Name, ClaimTypes.Role);
    identity.AddClaim(new Claim(ClaimTypes.Name, login));
    identity.AddClaim(new Claim(ClaimTypes.Role, role));
    await http.SignInAsync(CookieAuthenticationDefaults.AuthenticationScheme, new ClaimsPrincipal(identity));

    // Only ever a local path, never an address from the form.
    var returnUrl = form["returnUrl"].ToString();
    return Uri.IsWellFormedUriString(returnUrl, UriKind.Relative) && returnUrl.StartsWith('/') && !returnUrl.StartsWith("//")
        ? Results.LocalRedirect(returnUrl)
        : Results.Redirect("/");
}).DisableAntiforgery();   // checked above, so a stale form gets its own message

app.MapPost("/auth/logout", async (HttpContext http) =>
{
    await http.SignOutAsync(CookieAuthenticationDefaults.AuthenticationScheme);
    return Results.Redirect("/login");
});

app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.Run();
