namespace lancedb
{
    using System;

    /// <summary>
    /// Exception thrown when a LanceDB operation fails on the Rust side.
    /// </summary>
    public class LanceDbException : Exception
    {
        public LanceDbException(string message) : base(message) { }
        public LanceDbException(string message, Exception innerException) : base(message, innerException) { }
    }
}
