namespace ObjStore.SDK.Exceptions;

/// <summary>
/// Exception thrown when a requested policy is not found
/// </summary>
public class PolicyNotFoundException : ObjectStoreException
{
    /// <summary>
    /// Gets the ID of the policy that was not found
    /// </summary>
    public string PolicyId { get; }

    /// <summary>
    /// Gets the type of policy (e.g., "lifecycle", "replication")
    /// </summary>
    public string PolicyType { get; }

    /// <summary>
    /// Creates a new instance of PolicyNotFoundException
    /// </summary>
    /// <param name="policyId">The ID of the policy that was not found</param>
    /// <param name="policyType">The type of policy</param>
    public PolicyNotFoundException(string policyId, string policyType = "policy")
        : base($"{policyType} with ID '{policyId}' was not found", 404)
    {
        PolicyId = policyId;
        PolicyType = policyType;
    }

    /// <summary>
    /// Creates a new instance of PolicyNotFoundException with a custom message
    /// </summary>
    /// <param name="policyId">The ID of the policy that was not found</param>
    /// <param name="policyType">The type of policy</param>
    /// <param name="message">The error message</param>
    public PolicyNotFoundException(string policyId, string policyType, string message)
        : base(message, 404)
    {
        PolicyId = policyId;
        PolicyType = policyType;
    }

    /// <summary>
    /// Creates a new instance of PolicyNotFoundException with an inner exception
    /// </summary>
    /// <param name="policyId">The ID of the policy that was not found</param>
    /// <param name="policyType">The type of policy</param>
    /// <param name="message">The error message</param>
    /// <param name="innerException">The inner exception</param>
    public PolicyNotFoundException(string policyId, string policyType, string message, Exception innerException)
        : base(message, 404, innerException)
    {
        PolicyId = policyId;
        PolicyType = policyType;
    }
}
