namespace lancedb
{
    using System.Runtime.InteropServices;

    /// <summary>
    /// A Table is a collection of Records in a LanceDB Database.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A Table object is expected to be long lived and reused for multiple operations.
    /// Table objects will cache a certain amount of index data in memory. This cache
    /// will be freed when the Table is garbage collected. To eagerly free the cache you
    /// can call the <see cref="Close"/> method. Once the Table is closed, it cannot be
    /// used for any further operations.
    /// </para>
    /// <para>
    /// Closing a table is optional. If not closed, it will be closed when it is garbage
    /// collected.
    /// </para>
    /// </remarks>
    public class Table : IDisposable
    {
        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern RustStringHandle table_get_name(IntPtr table_ptr);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern bool table_is_open(IntPtr table_ptr);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr table_create_query(IntPtr table_ptr);

        private TableHandle? _handle;

        internal Table(IntPtr tablePtr)
        {
            _handle = new TableHandle(tablePtr);
        }

        /// <summary>
        /// The name of the table.
        /// </summary>
        public string Name
        {
            get
            {
                using var nameHandle = table_get_name(_handle!.DangerousGetHandle());
                return nameHandle.AsString();
            }
        }

        /// <summary>
        /// Return <c>true</c> if the table has not been closed.
        /// </summary>
        /// <returns><c>true</c> if the table is open; otherwise, <c>false</c>.</returns>
        public bool IsOpen() => _handle != null && !_handle.IsInvalid && !_handle.IsClosed;

        /// <summary>
        /// Close the table and free any resources associated with it.
        /// </summary>
        /// <remarks>
        /// It is safe to call this method multiple times.
        /// Any attempt to use the table after it has been closed will raise an error.
        /// </remarks>
        public void Close()
        {
            Dispose();
        }

        public void Dispose()
        {
            _handle?.Dispose();
            _handle = null;
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Create a <see cref="lancedb.Query"/> builder.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Queries allow you to search your existing data. By default the query will
        /// return all the data in the table in no particular order. The builder
        /// returned by this method can be used to control the query using filtering,
        /// vector similarity, sorting, and more.
        /// </para>
        /// <para>
        /// By default, all columns are returned. For best performance, you should
        /// only fetch the columns you need.
        /// </para>
        /// <para>
        /// When appropriate, various indices and statistics will be used to accelerate
        /// the query.
        /// </para>
        /// </remarks>
        /// <returns>A builder that can be used to parameterize the query.</returns>
        public Query Query()
        {
            IntPtr queryPtr = table_create_query(_handle!.DangerousGetHandle());
            return new Query(queryPtr);
        }
    }
}