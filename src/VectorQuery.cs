namespace lancedb
{
    using System;
    using System.Runtime.InteropServices;
    using System.Text;

    public class VectorQuery : QueryBase
    {
        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr vector_query_column(IntPtr vector_query_ptr, IntPtr column_name);

        public VectorQuery(IntPtr vectorQueryPtr)
            : base(vectorQueryPtr)
        {
        }

        public VectorQuery Column(string column)
        {
            byte[] columnUtf8Bytes = Encoding.UTF8.GetBytes(column);
            unsafe
            {
                fixed (byte* columnBytePtr = columnUtf8Bytes)
                {
                    vector_query_column(_queryPtr, new IntPtr(columnBytePtr));
                }
            }

            return this;
        }
    }
}