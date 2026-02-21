namespace lancedb
{
    using System;

    /// <summary>
    /// Exception thrown when a LanceDB native operation fails.
    /// </summary>
    public class LanceDbException : Exception
    {
        public LanceDbException(string message) : base(message) { }
        public LanceDbException(string message, Exception innerException) : base(message, innerException) { }
    }
}
