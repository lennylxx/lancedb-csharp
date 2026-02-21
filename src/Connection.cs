namespace lancedb
{
    using System;
    using System.Runtime.InteropServices;
    using System.Text;

    public class Connection
    {
        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void database_connect(IntPtr uri, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void database_close(IntPtr connection_ptr);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void database_open_table(IntPtr connection_ptr, IntPtr table_name, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void database_create_empty_table(IntPtr connection_ptr, IntPtr table_name, NativeCall.FfiCallback completion);

        private IntPtr _connectionPtr;

        public Connection()
        {
        }

        /// <summary>
        /// Connect to a LanceDB instance at the given URI.
        ///
        /// Accepted formats:
        ///
        /// - `/path/to/database` - local database
        /// - `s3://bucket/path/to/database` or `gs://bucket/path/to/database` - database on cloud storage
        /// - `db://host:port` - remote database (LanceDB cloud)
        /// </summary>
        /// <param name="uri">The uri of the database. If the database uri starts with db:// then it connects to a remote database.</param>
        /// <param name="opts">The <see cref="ConnectionOptions"/> to use when connecting to the database.</param>
        public async Task Connect(string uri, ConnectionOptions? opts = null)
        {
            byte[] uriBytes = NativeCall.ToUtf8(uri);
            _connectionPtr = await NativeCall.Async(callback =>
            {
                unsafe
                {
                    fixed (byte* p = uriBytes)
                    {
                        database_connect(new IntPtr(p), callback);
                    }
                }
            });
        }

        public void Close()
        {
            database_close(_connectionPtr);
        }

        /// <summary>
        /// Open a table in the database.
        /// </summary>
        /// <param name="name">The name of the table</param>
        public async Task<Table> OpenTable(string name, OpenTableOptions? options = null)
        {
            byte[] nameBytes = NativeCall.ToUtf8(name);
            IntPtr tablePtr = await NativeCall.Async(callback =>
            {
                unsafe
                {
                    fixed (byte* p = nameBytes)
                    {
                        database_open_table(_connectionPtr, new IntPtr(p), callback);
                    }
                }
            });
            return new Table(tablePtr);
        }

        /// <summary>
        /// Create an empty table with the given name and a minimal schema.
        /// </summary>
        /// <param name="name">The name of the table</param>
        public async Task<Table> CreateEmptyTable(string name)
        {
            byte[] nameBytes = NativeCall.ToUtf8(name);
            IntPtr tablePtr = await NativeCall.Async(callback =>
            {
                unsafe
                {
                    fixed (byte* p = nameBytes)
                    {
                        database_create_empty_table(_connectionPtr, new IntPtr(p), callback);
                    }
                }
            });
            return new Table(tablePtr);
        }
    }
}