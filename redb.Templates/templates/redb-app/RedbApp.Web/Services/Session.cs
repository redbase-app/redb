using RedbApp.Models;

namespace RedbApp.Web.Services;

/// <summary>
/// Who is signed in, and the token that proves it. Kept in memory only: reloading the page signs out.
/// A real application may keep the token in sessionStorage through JS interop.
/// </summary>
public sealed class Session
{
    public string? Token { get; private set; }
    public string? Login { get; private set; }
    public string? Role { get; private set; }

    public bool IsSignedIn => Token is not null;

    public event Action? Changed;

    public void SignIn(LoginResponse response)
    {
        Token = response.Token;
        Login = response.Login;
        Role = response.Role;
        Changed?.Invoke();
    }

    public void SignOut()
    {
        Token = Login = Role = null;
        Changed?.Invoke();
    }
}
