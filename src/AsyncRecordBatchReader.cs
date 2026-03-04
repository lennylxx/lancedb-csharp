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
    /// Implements <see cref="IAsyncEnumerable{T}"/> so it can be consumed with
    /// <c>await foreach</c>. The reader must be disposed after use.
    /// </remarks>
    public abstract class AsyncRecordBatchReader : IAsyncEnumerable<RecordBatch>, IDisposable
    {
        /// <inheritdoc/>
        public abstract IAsyncEnumerator<RecordBatch> GetAsyncEnumerator(
            CancellationToken cancellationToken = default);

        /// <inheritdoc/>
        public virtual void Dispose() { }

        /// <summary>
        /// Creates a reader backed by a native stream from Rust.
        /// </summary>
        internal static AsyncRecordBatchReader FromNativeStream(IntPtr handle)
        {
            return new NativeStreamReader(handle);
        }

        /// <summary>
        /// Creates a reader that yields batches from a materialized <see cref="RecordBatch"/>.
        /// </summary>
        internal static AsyncRecordBatchReader FromRecordBatch(RecordBatch result, int? maxBatchLength = null)
        {
            return new MaterializedBatchReader(result, maxBatchLength);
        }
    }

    /// <summary>
    /// Reads batches one at a time from a native Rust stream via FFI.
    /// </summary>
    internal sealed class NativeStreamReader : AsyncRecordBatchReader
    {
        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void stream_next(
            IntPtr stream_ptr, NativeCall.FfiCallback completion);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void stream_close(IntPtr stream_ptr);

        private IntPtr _handle;
        private bool _disposed;

        internal NativeStreamReader(IntPtr handle)
        {
            _handle = handle;
        }

        public override async IAsyncEnumerator<RecordBatch> GetAsyncEnumerator(
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

        public override void Dispose()
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

    /// <summary>
    /// Wraps an already-materialized <see cref="RecordBatch"/> as a streaming reader,
    /// optionally slicing it into smaller batches.
    /// </summary>
    internal sealed class MaterializedBatchReader : AsyncRecordBatchReader
    {
        private readonly RecordBatch _result;
        private readonly int? _maxBatchLength;

        internal MaterializedBatchReader(RecordBatch result, int? maxBatchLength = null)
        {
            _result = result;
            _maxBatchLength = maxBatchLength;
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators
        public override async IAsyncEnumerator<RecordBatch> GetAsyncEnumerator(
            CancellationToken cancellationToken = default)
#pragma warning restore CS1998
        {
            if (!_maxBatchLength.HasValue || _result.Length <= _maxBatchLength.Value)
            {
                yield return _result;
                yield break;
            }

            int offset = 0;
            while (offset < _result.Length)
            {
                cancellationToken.ThrowIfCancellationRequested();
                int length = Math.Min(_maxBatchLength.Value, _result.Length - offset);
                yield return SliceBatch(_result, offset, length);
                offset += length;
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
    }
}
