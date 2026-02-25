using System.Collections.Generic;

namespace redb.Core.Models.Users
{
    /// <summary>
    /// User data validation result
    /// </summary>
    public class UserValidationResult
    {
        /// <summary>
        /// Validation passed successfully
        /// </summary>
        public bool IsValid { get; set; }
        
        /// <summary>
        /// List of validation errors
        /// </summary>
        public List<ValidationError> Errors { get; set; } = new();
        
        /// <summary>
        /// Add validation error
        /// </summary>
        public void AddError(string field, string message)
        {
            Errors.Add(new ValidationError { Field = field, Message = message });
            IsValid = false;
        }
        
        /// <summary>
        /// Add validation error
        /// </summary>
        public void AddError(ValidationError error)
        {
            Errors.Add(error);
            IsValid = false;
        }
        
        /// <summary>
        /// Create successful validation result
        /// </summary>
        public static UserValidationResult Success()
        {
            return new UserValidationResult { IsValid = true };
        }
        
        /// <summary>
        /// Create validation result with error
        /// </summary>
        public static UserValidationResult WithError(string field, string message)
        {
            var result = new UserValidationResult();
            result.AddError(field, message);
            return result;
        }
    }
    
    /// <summary>
    /// Validation error
    /// </summary>
    public class ValidationError
    {
        /// <summary>
        /// Field with error
        /// </summary>
        public string Field { get; set; } = "";
        
        /// <summary>
        /// Error message
        /// </summary>
        public string Message { get; set; } = "";
        
        /// <summary>
        /// Error code (optional)
        /// </summary>
        public string? ErrorCode { get; set; }
    }
}
