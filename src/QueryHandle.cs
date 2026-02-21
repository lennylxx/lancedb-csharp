namespace lancedb
{
    using System.Runtime.InteropServices;

    /// <summary>
    /// SafeHandle wrapper for a Rust Query pointer.
    /// Automatically calls query_free when the handle is released.
    /// </summary>
    internal class QueryHandle : SafeHandle
    {
        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void query_free(IntPtr query_ptr);

        public QueryHandle() : base(IntPtr.Zero, true) { }
        public QueryHandle(IntPtr ptr) : base(ptr, true) { }

        public override bool IsInvalid => handle == IntPtr.Zero;

        protected override bool ReleaseHandle()
        {
            if (!IsInvalid)
            {
                query_free(handle);
            }
            return true;
        }
    }
}
