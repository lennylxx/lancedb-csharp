namespace lancedb
{
    using System;

    /// <summary>
    /// Base class for all queries (scan and vector).
    /// </summary>
    /// <remarks>
    /// This class is not intended to be created directly. Instead, use the
    /// <see cref="Table.Query"/> method to create a query.
    /// Implements <see cref="IDisposable"/> to release the underlying native query handle.
    /// </remarks>
    public class QueryBase : IDisposable
    {
        private QueryHandle? _handle;

        protected IntPtr QueryPtr => _handle!.DangerousGetHandle();

        internal QueryBase(IntPtr queryPtr)
        {
            _handle = new QueryHandle(queryPtr);
        }

        public void Dispose()
        {
            _handle?.Dispose();
            _handle = null;
            GC.SuppressFinalize(this);
        }
    }
}