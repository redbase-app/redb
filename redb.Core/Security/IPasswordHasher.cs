namespace redb.Core.Security;

/// <summary>
/// Interface for password hashing operations.
/// Implementations should use secure hashing algorithms (bcrypt, PBKDF2, etc.)
/// </summary>
public interface IPasswordHasher
{
    /// <summary>
    /// Hash a plain-text password.
    /// </summary>
    /// <param name="password">Plain-text password</param>
    /// <returns>Hashed password</returns>
    string HashPassword(string password);
    
    /// <summary>
    /// Verify a password against a hash.
    /// </summary>
    /// <param name="password">Plain-text password to verify</param>
    /// <param name="hashedPassword">Stored hashed password</param>
    /// <returns>True if password matches</returns>
    bool VerifyPassword(string password, string hashedPassword);
}

