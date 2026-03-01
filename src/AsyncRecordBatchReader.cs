namespace lancedb
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.InteropServices;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Arrow;
    using Apache.Arrow.C;

    /// <summary>
    /// A streaming reader that yields <see cref="RecordBatch"/> results
    /// one batch at a time from a query execution.
    /// </summary>
    /// <remarks>
    /// Created by <see cref="QueryBase{T}.ToBatches"/>. Implements
    /// <see cref="IAsyncEnumerable{T}"/> so it can be consumed with
    /// <c>await foreach</c>. The reader must be disposed after use to
    /// release the underlying native stream handle.
    /// </remarks>
    public sealed class AsyncRecordBatchReader : IAsyncEnumerable<RecordBatch>, IDisposable
    {
        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void stream_next(
            IntPtr stream_ptr, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void stream_close(IntPtr stream_ptr);

        private IntPtr _handle;
        private bool _disposed;

        internal AsyncRecordBatchReader(IntPtr handle)
        {
            _handle = handle;
        }

        /// <summary>
        /// Returns an async enumerator that yields <see cref="RecordBatch"/>
        /// results from the stream.
        /// </summary>
        public async IAsyncEnumerator<RecordBatch> GetAsyncEnumerator(
            CancellationToken cancellationToken = default)
        {
            while (!_disposed)
            {
                cancellationToken.ThrowIfCancellationRequested();

                IntPtr result = await NativeCall.Async(completion =>
                {
                    stream_next(_handle, completion);
                }).ConfigureAwait(false);

                if (result == IntPtr.Zero)
                {
                    yield break;
                }

                yield return ImportBatch(result);
            }
        }

        private static unsafe RecordBatch ImportBatch(IntPtr cdataPtr)
        {
            try
            {
                var arrayPtr = Marshal.ReadIntPtr(cdataPtr);
                var schemaPtr = Marshal.ReadIntPtr(cdataPtr + IntPtr.Size);

                var schema = CArrowSchemaImporter.ImportSchema(
                    (CArrowSchema*)schemaPtr);
                return CArrowArrayImporter.ImportRecordBatch(
                    (CArrowArray*)arrayPtr, schema);
            }
            finally
            {
                NativeCall.free_ffi_cdata(cdataPtr);
            }
        }

        /// <summary>
        /// Releases the underlying native stream handle.
        /// </summary>
        public void Dispose()
        {
            if (!_disposed)
            {
                _disposed = true;
                if (_handle != IntPtr.Zero)
                {
                    stream_close(_handle);
                    _handle = IntPtr.Zero;
                }
            }
        }
    }
}
