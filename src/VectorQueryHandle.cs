namespace lancedb
{
    using System.Runtime.InteropServices;

    /// <summary>
    /// SafeHandle wrapper for a Rust VectorQuery pointer.
    /// Automatically calls vector_query_free when the handle is released.
    /// </summary>
    internal class VectorQueryHandle : SafeHandle
    {
        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void vector_query_free(IntPtr vector_query_ptr);

        public VectorQueryHandle() : base(IntPtr.Zero, true) { }
        public VectorQueryHandle(IntPtr ptr) : base(ptr, true) { }

        public override bool IsInvalid => handle == IntPtr.Zero;

        protected override bool ReleaseHandle()
        {
            if (!IsInvalid)
            {
                vector_query_free(handle);
            }
            return true;
        }
    }
}
