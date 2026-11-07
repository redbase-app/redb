using System;
using System.Security.Cryptography;
using System.Text;

namespace redb.Core.Security;

/// <summary>
/// Password hasher using BCrypt (recommended for production).
/// Backward-compatible: can verify legacy SHA256+salt hashes from <see cref="SimplePasswordHasher"/>.
/// New passwords are always hashed with BCrypt.
/// </summary>
public class BcryptPasswordHasher : IPasswordHasher
{
    private readonly int _workFactor;

    /// <summary>
    /// Creates a BCrypt password hasher.
    /// </summary>
    /// <param name="workFactor">BCrypt work factor (default: 12, ~250ms per hash).</param>
    public BcryptPasswordHasher(int workFactor = 12)
    {
        if (workFactor < 4 || workFactor > 31)
            throw new ArgumentOutOfRangeException(nameof(workFactor), workFactor, "Work factor must be between 4 and 31.");
        _workFactor = workFactor;
    }

    /// <inheritdoc />
    public string HashPassword(string password)
    {
        if (string.IsNullOrEmpty(password))
            throw new ArgumentException("Password cannot be empty", nameof(password));

        return BCrypt.Net.BCrypt.HashPassword(password, _workFactor);
    }

    /// <inheritdoc />
    public bool VerifyPassword(string password, string hashedPassword)
    {
        if (string.IsNullOrEmpty(password) || string.IsNullOrEmpty(hashedPassword))
            return false;

        try
        {
            // BCrypt hashes start with "$2a$", "$2b$", or "$2y$"
            if (hashedPassword.StartsWith("$2", StringComparison.Ordinal))
            {
                return BCrypt.Net.BCrypt.Verify(password, hashedPassword);
            }

            // Legacy SHA256+salt format: "base64(salt):base64(hash)"
            if (hashedPassword.Contains(':'))
            {
                return VerifyLegacySha256(password, hashedPassword);
            }

            return false;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Checks whether the stored hash is in legacy SHA256 format and needs rehashing to BCrypt.
    /// Use this in login flow: if true, rehash with <see cref="HashPassword"/> after successful verify.
    /// </summary>
    public static bool NeedsRehash(string hashedPassword)
    {
        if (string.IsNullOrEmpty(hashedPassword))
            return true;

        // Legacy format (SHA256+salt) or unknown → needs rehash
        return !hashedPassword.StartsWith("$2", StringComparison.Ordinal);
    }

    /// <summary>
    /// Verify password against legacy SHA256+salt format from <see cref="SimplePasswordHasher"/>.
    /// </summary>
    private static bool VerifyLegacySha256(string password, string hashedPassword)
    {
        var parts = hashedPassword.Split(':');
        if (parts.Length != 2)
            return false;

        var salt = Convert.FromBase64String(parts[0]);
        var storedHash = Convert.FromBase64String(parts[1]);

        var passwordBytes = Encoding.UTF8.GetBytes(password);
        var saltedPassword = new byte[passwordBytes.Length + salt.Length];
        Array.Copy(passwordBytes, 0, saltedPassword, 0, passwordBytes.Length);
        Array.Copy(salt, 0, saltedPassword, passwordBytes.Length, salt.Length);

        var computedHash = SHA256.HashData(saltedPassword);

        return CryptographicOperations.FixedTimeEquals(storedHash, computedHash);
    }
}
