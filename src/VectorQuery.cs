namespace lancedb
{
    using System;
    using System.Runtime.InteropServices;

    /// <summary>
    /// A builder for vector queries, created by <see cref="Query.NearestTo"/>.
    /// </summary>
    public class VectorQuery : QueryBase
    {
        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr vector_query_column(IntPtr vector_query_ptr, IntPtr column_name);

        public VectorQuery(IntPtr vectorQueryPtr)
            : base(vectorQueryPtr)
        {
        }

        /// <summary>
        /// Set the vector column to query.
        /// </summary>
        /// <remarks>
        /// This controls which column is compared to the query vector supplied in
        /// the call to <see cref="Query.NearestTo"/>.
        /// This parameter must be specified if the table has more than one column
        /// whose data type is a fixed-size-list of floats.
        /// </remarks>
        /// <param name="column">The name of the vector column.</param>
        /// <returns>This <see cref="VectorQuery"/> instance for method chaining.</returns>
        public VectorQuery Column(string column)
        {
            byte[] columnBytes = NativeCall.ToUtf8(column);
            unsafe
            {
                fixed (byte* p = columnBytes)
                {
                    vector_query_column(QueryPtr, new IntPtr(p));
                }
            }

            return this;
        }
    }
}