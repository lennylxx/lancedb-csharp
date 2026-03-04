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
    /// Created by <see cref="QueryBase{T}.ToBatches"/> (native stream) or
    /// <see cref="HybridQuery.ToBatches"/> (materialized result). Implements
    /// <see cref="IAsyncEnumerable{T}"/> so it can be consumed with
    /// <c>await foreach</c>. The reader must be disposed after use to
    /// release any underlying native stream handle.
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
        private readonly RecordBatch? _materializedResult;
        private readonly int? _maxBatchLength;

        /// <summary>
        /// Creates a reader backed by a native stream handle from Rust.
        /// </summary>
        internal AsyncRecordBatchReader(IntPtr handle)
        {
            _handle = handle;
        }

        /// <summary>
        /// Creates a reader that yields batches from a materialized <see cref="RecordBatch"/>.
        /// Used by <see cref="HybridQuery.ToBatches"/> where the full result is already
        /// computed in C# (hybrid merging requires all rows).
        /// </summary>
        internal AsyncRecordBatchReader(RecordBatch result, int? maxBatchLength = null)
        {
            _materializedResult = result;
            _maxBatchLength = maxBatchLength;
        }

        /// <summary>
        /// Returns an async enumerator that yields <see cref="RecordBatch"/>
        /// results from either a native stream or a materialized result.
        /// </summary>
        public async IAsyncEnumerator<RecordBatch> GetAsyncEnumerator(
            CancellationToken cancellationToken = default)
        {
            if (_materializedResult != null)
            {
                if (!_maxBatchLength.HasValue || _materializedResult.Length <= _maxBatchLength.Value)
                {
                    yield return _materializedResult;
                }
                else
                {
                    int offset = 0;
                    while (offset < _materializedResult.Length)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        int length = Math.Min(_maxBatchLength.Value, _materializedResult.Length - offset);
                        yield return SliceBatch(_materializedResult, offset, length);
                        offset += length;
                    }
                }
                yield break;
            }

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

        private static RecordBatch SliceBatch(RecordBatch batch, int offset, int length)
        {
            var arrays = new IArrowArray[batch.ColumnCount];
            for (int i = 0; i < batch.ColumnCount; i++)
            {
                arrays[i] = ((Apache.Arrow.Array)batch.Column(i)).Slice(offset, length);
            }
            return new RecordBatch(batch.Schema, arrays, length);
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
