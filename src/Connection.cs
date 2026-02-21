namespace lancedb
{
    using System;
    using System.Runtime.InteropServices;
    using System.Text;

    public class Connection
    {
        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        public delegate void IntPtrCallback(IntPtr ptr);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        public static extern void database_connect(IntPtr uri, IntPtrCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        public static extern void database_close(IntPtr connection_ptr);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        public static extern void database_open_table(IntPtr connection_ptr, IntPtr table_name, IntPtrCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        public static extern void database_create_empty_table(IntPtr connection_ptr, IntPtr table_name, IntPtrCallback completion);

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
            var tcs = new TaskCompletionSource<IntPtr>();
            IntPtrCallback completion = (ptr) => tcs.SetResult(ptr);
            GCHandle completionHandle = GCHandle.Alloc(completion, GCHandleType.Normal);
            byte[] uriUtf8Bytes = Encoding.UTF8.GetBytes(uri);

            unsafe
            {
                fixed (byte* uriBytePtr = uriUtf8Bytes)
                {
                    database_connect(new IntPtr(uriBytePtr), completion);
                }
            }

            IntPtr connectionPtr = await tcs.Task.ConfigureAwait(false);
            _connectionPtr = connectionPtr;
            completionHandle.Free();
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
            var tcs = new TaskCompletionSource<IntPtr>();
            IntPtrCallback completion = (ptr) => tcs.SetResult(ptr);
            GCHandle completionHandle = GCHandle.Alloc(completion, GCHandleType.Normal);
            byte[] nameUtf8Bytes = Encoding.UTF8.GetBytes(name);

            unsafe
            {
                fixed (byte* nameBytePtr = nameUtf8Bytes)
                {
                    database_open_table(_connectionPtr, new IntPtr(nameBytePtr), completion);
                }
            }

            IntPtr tablePtr = await tcs.Task.ConfigureAwait(false);
            completionHandle.Free();

            return new Table(tablePtr);
        }

        /// <summary>
        /// Create an empty table with the given name and a minimal schema.
        /// </summary>
        /// <param name="name">The name of the table</param>
        public async Task<Table> CreateEmptyTable(string name)
        {
            var tcs = new TaskCompletionSource<IntPtr>();
            IntPtrCallback completion = (ptr) => tcs.SetResult(ptr);
            GCHandle completionHandle = GCHandle.Alloc(completion, GCHandleType.Normal);
            byte[] nameUtf8Bytes = Encoding.UTF8.GetBytes(name);

            unsafe
            {
                fixed (byte* nameBytePtr = nameUtf8Bytes)
                {
                    database_create_empty_table(_connectionPtr, new IntPtr(nameBytePtr), completion);
                }
            }

            IntPtr tablePtr = await tcs.Task.ConfigureAwait(false);
            completionHandle.Free();

            return new Table(tablePtr);
        }
    }
}