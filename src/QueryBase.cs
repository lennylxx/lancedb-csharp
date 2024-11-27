namespace lancedb
{
    using System;

    /// <summary>
    /// Common parameters that can be applied to scans and vector queries
    /// </summary>
    public class QueryBase
    {
        protected IntPtr _queryPtr { get; }

        internal QueryBase(IntPtr queryPtr)
        {
            _queryPtr = queryPtr;
        }
    }
}