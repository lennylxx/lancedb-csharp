namespace lancedb
{
    using System.Runtime.InteropServices;

    /// <summary>
    /// A Table is a collection of Records in a LanceDB Database.
    ///
    /// A Table object is expected to be long lived and reused for multiple operations.
    /// Table objects will cache a certain amount of index data in memory. This cache
    /// will be freed when the Table is garbage collected. To eagerly free the cache you
    /// can call the `Close` method. Once the Table is closed, it cannot be used for any
    /// further operations.
    ///
    /// Closing a table is optional. If not closed, it will be closed when it is garbage
    /// collected.
    /// </summary>
    public class Table : IDisposable
    {
        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        public static extern RustStringHandle table_get_name(IntPtr table_ptr);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        public static extern bool table_is_open(IntPtr table_ptr);


        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr table_create_query(IntPtr table_ptr);

        private IntPtr _tablePtr { get; }

        private RustStringHandle _name;

        internal Table(IntPtr tablePtr)
        {
            _tablePtr = tablePtr;
        }

        ~Table()
        {
        }

        public void Dispose()
        {
            _name.Dispose();
        }

        /// <summary>
        /// Returns the name of the table.
        /// </summary>
        public string Name
        {
            get
            {
                _name = table_get_name(_tablePtr);
                return _name.AsString();
            }
        }

        /// <summary>
        /// Return true if the table has not been closed.
        /// </summary>
        public bool IsOpen() => table_is_open(_tablePtr);

        /// <summary>
        /// Close the table, releasing any underlying resources.
        ///
        /// It is safe to call this method multiple times.
        ///
        /// Any attempt to use the table after it is closed will result in an error.
        /// </summary>
        public void Close() => throw new NotImplementedException();

        /// <summary>
        /// Return a brief description of the table.
        /// </summary>
        public string Display() => throw new NotImplementedException();

        /// <summary>
        /// Get the schema of the table.
        /// </summary>
        // public Task<Schema> Schema() => throw new NotImplementedException();

        /// <summary>
        /// Create a <see cref="Query" /> Builder.
        ///
        /// Queries allow you to search your existing data.  By default the query will
        /// return all the data in the table in no particular order.  The builder
        /// returned by this method can be used to control the query using filtering,
        /// vector similarity, sorting, and more.
        ///
        /// Note: By default, all columns are returned.  For best performance, you should
        /// only fetch the columns you need.  See [`Query::select_with_projection`] for
        /// more details.
        ///
        /// When appropriate, various indices and statistics will be used to accelerate
        /// the query.
        /// </summary>
        /// <returns>A builder that can be used to parameterize the query</returns>
        public Query Query()
        {
            IntPtr queryPtr = table_create_query(_tablePtr);
            return new Query(queryPtr);
        }
    }
}