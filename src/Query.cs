namespace lancedb
{
    using System.Runtime.InteropServices;

    public class Query : QueryBase
    {
        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr query_nearest_to(IntPtr query_ptr, double[] vector, UIntPtr len);

        internal Query(IntPtr queryPtr)
            : base(queryPtr)
        {
        }

        public VectorQuery NearestTo(double[] vector)
        {
            IntPtr _vectorQueryPtr = query_nearest_to(_queryPtr, vector, (UIntPtr)vector.Length);
            return new VectorQuery(_vectorQueryPtr);
        }
    }
}