namespace lancedb
{
    using System;
    using System.Runtime.InteropServices;
    using System.Text;
    using System.Threading.Tasks;

    /// <summary>
    /// Centralized helpers for calling Rust FFI functions with proper error handling.
    /// </summary>
    internal static class NativeCall
    {
        /// <summary>
        /// Unified callback delegate for async FFI operations.
        /// On success, result is set and error is IntPtr.Zero.
        /// On error, result is IntPtr.Zero and error points to a UTF-8 error string
        /// (caller must free with free_string).
        /// </summary>
        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate void FfiCallback(IntPtr result, IntPtr error);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void free_string(IntPtr ptr);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        internal static extern void free_ffi_bytes(IntPtr ptr);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr ffi_get_last_error();

        /// <summary>
        /// Calls an async FFI function that uses the unified (result, error) callback pattern.
        /// Returns the result IntPtr on success, or throws LanceDbException on error.
        /// </summary>
        internal static async Task<IntPtr> Async(Action<FfiCallback> invoke)
        {
            var tcs = new TaskCompletionSource<IntPtr>();

            FfiCallback callback = (result, error) =>
            {
                if (error != IntPtr.Zero)
                {
                    string message = ReadUtf8AndFree(error);
                    tcs.SetException(new LanceDbException(message));
                }
                else
                {
                    tcs.SetResult(result);
                }
            };

            GCHandle handle = GCHandle.Alloc(callback, GCHandleType.Normal);
            try
            {
                invoke(callback);
                return await tcs.Task.ConfigureAwait(false);
            }
            finally
            {
                handle.Free();
            }
        }

        /// <summary>
        /// Reads a UTF-8 C string from an IntPtr and frees it via free_string.
        /// </summary>
        private static string ReadUtf8AndFree(IntPtr ptr)
        {
            try
            {
#if NETSTANDARD2_0
                if (ptr == IntPtr.Zero)
                {
                    return string.Empty;
                }

                int len = 0;
                unsafe
                {
                    byte* p = (byte*)ptr;
                    while (p[len] != 0) { len++; }
                }
                byte[] bytes = new byte[len];
                Marshal.Copy(ptr, bytes, 0, len);
                return Encoding.UTF8.GetString(bytes);
#else
                return Marshal.PtrToStringUTF8(ptr) ?? string.Empty;
#endif
            }
            finally
            {
                free_string(ptr);
            }
        }

        /// <summary>
        /// Reads a UTF-8 C string from an IntPtr and frees it via free_string.
        /// For use by callers outside NativeCall that receive string results from FFI.
        /// </summary>
        internal static string ReadStringAndFree(IntPtr ptr)
        {
            return ReadUtf8AndFree(ptr);
        }

        /// <summary>
        /// Encodes a string as null-terminated UTF-8 bytes for passing to Rust FFI.
        /// </summary>
        internal static byte[] ToUtf8(string s)
        {
            byte[] utf8 = Encoding.UTF8.GetBytes(s);
            byte[] nullTerminated = new byte[utf8.Length + 1];
            utf8.CopyTo(nullTerminated, 0);
            return nullTerminated;
        }

        /// <summary>
        /// Checks if a synchronous FFI call returned null due to an error.
        /// If a thread-local error was set by the Rust FFI, throws LanceDbException.
        /// </summary>
        internal static void ThrowIfNullWithError(IntPtr ptr, string fallbackMessage)
        {
            if (ptr != IntPtr.Zero)
            {
                return;
            }

            IntPtr errorPtr = ffi_get_last_error();
            if (errorPtr != IntPtr.Zero)
            {
                string message = ReadStringAndFree(errorPtr);
                throw new LanceDbException(message);
            }

            throw new LanceDbException(fallbackMessage);
        }
    }
}
