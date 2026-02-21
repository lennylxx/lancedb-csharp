namespace lancedb
{
    using System.Runtime.InteropServices;

    /// <summary>
    /// SafeHandle wrapper for a Rust Connection pointer.
    /// Automatically calls database_close when the handle is released.
    /// </summary>
    internal class ConnectionHandle : SafeHandle
    {
        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void database_close(IntPtr connection_ptr);

        public ConnectionHandle() : base(IntPtr.Zero, true) { }
        public ConnectionHandle(IntPtr ptr) : base(ptr, true) { }

        public override bool IsInvalid => handle == IntPtr.Zero;

        protected override bool ReleaseHandle()
        {
            if (!IsInvalid)
            {
                database_close(handle);
            }
            return true;
        }
    }
}
