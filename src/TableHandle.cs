namespace lancedb
{
    using System;
    using System.Runtime.InteropServices;

    /// <summary>
    /// SafeHandle wrapper for a Rust Table pointer.
    /// Automatically calls table_close when the handle is released.
    /// </summary>
    internal class TableHandle : SafeHandle
    {
        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void table_close(IntPtr table_ptr);

        public TableHandle() : base(IntPtr.Zero, true) { }
        public TableHandle(IntPtr ptr) : base(ptr, true) { }

        public override bool IsInvalid => handle == IntPtr.Zero;

        protected override bool ReleaseHandle()
        {
            if (!IsInvalid)
            {
                table_close(handle);
            }
            return true;
        }
    }
}
