using System;
using System.Security.Cryptography;
using System.Text;

namespace redb.Core.Security;

/// <summary>
/// Password hasher implementation using SHA256 + salt.
/// Implements IPasswordHasher interface for DI.
/// FUTURE: Replace with BCrypt for better security.
/// </summary>
public class SimplePasswordHasher : IPasswordHasher
{
    /// <summary>
    /// Hash password with salt.
    /// </summary>
    /// <param name="password">Plain-text password</param>
    /// <returns>Hashed password with salt in format: base64(salt):base64(hash)</returns>
    public string HashPassword(string password)
    {
        if (string.IsNullOrEmpty(password))
            throw new ArgumentException("Password cannot be empty", nameof(password));

        var salt = GenerateSalt();
        var hash = ComputeHash(password, salt);
        
        return $"{Convert.ToBase64String(salt)}:{Convert.ToBase64String(hash)}";
    }
    
    /// <summary>
    /// Verify password against stored hash.
    /// </summary>
    /// <param name="password">Plain-text password to verify</param>
    /// <param name="hashedPassword">Stored hashed password from DB</param>
    /// <returns>True if password matches</returns>
    public bool VerifyPassword(string password, string hashedPassword)
    {
        if (string.IsNullOrEmpty(password) || string.IsNullOrEmpty(hashedPassword))
            return false;

        try
        {
            var parts = hashedPassword.Split(':');
            if (parts.Length != 2)
                return false;

            var salt = Convert.FromBase64String(parts[0]);
            var storedHash = Convert.FromBase64String(parts[1]);
            var computedHash = ComputeHash(password, salt);
            
            return ConstantTimeEquals(storedHash, computedHash);
        }
        catch
        {
            return false;
        }
    }
    
    /// <summary>
    /// Generate random salt (256 bits).
    /// </summary>
    private static byte[] GenerateSalt()
    {
        var salt = new byte[32];
        using var rng = RandomNumberGenerator.Create();
        rng.GetBytes(salt);
        return salt;
    }
    
    /// <summary>
    /// Compute SHA256 hash of password with salt.
    /// </summary>
    private static byte[] ComputeHash(string password, byte[] salt)
    {
        var passwordBytes = Encoding.UTF8.GetBytes(password);
        var saltedPassword = new byte[passwordBytes.Length + salt.Length];
        
        Array.Copy(passwordBytes, 0, saltedPassword, 0, passwordBytes.Length);
        Array.Copy(salt, 0, saltedPassword, passwordBytes.Length, salt.Length);
        
        return SHA256.HashData(saltedPassword);
    }
    
    /// <summary>
    /// Constant-time comparison to prevent timing attacks.
    /// </summary>
    private static bool ConstantTimeEquals(byte[] a, byte[] b)
    {
        if (a.Length != b.Length)
            return false;

        var result = 0;
        for (int i = 0; i < a.Length; i++)
        {
            result |= a[i] ^ b[i];
        }
        
        return result == 0;
    }
}

