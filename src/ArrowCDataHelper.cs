namespace lancedb
{
    using System;
    using System.Runtime.InteropServices;
    using Apache.Arrow;
    using Apache.Arrow.C;

    /// <summary>
    /// Helpers for marshalling Arrow data across the C Data Interface — both
    /// exporting managed RecordBatches to native code and importing
    /// native-produced batches back into managed memory.
    /// </summary>
    internal static class ArrowCDataHelper
    {
        /// <summary>
        /// Enables exporting managed (GC heap) memory through the Arrow C Data Interface.
        /// Must be set before any export calls.
        /// </summary>
        static ArrowCDataHelper()
        {
            CArrowArrayExporter.EnableManagedMemoryExport = true;
        }

        /// <summary>
        /// Deep-clones a RecordBatch into managed byte[] buffers suitable for
        /// non-destructive C Data Interface export. The original batch remains
        /// intact after the clone is exported.
        /// </summary>
        internal static RecordBatch CloneBatchForExport(RecordBatch batch)
        {
            var columns = new IArrowArray[batch.ColumnCount];
            for (int i = 0; i < batch.ColumnCount; i++)
            {
                columns[i] = ArrowArrayFactory.BuildArray(
                    CloneArrayData(batch.Column(i).Data));
            }
            return new RecordBatch(batch.Schema, columns, batch.Length);
        }

        /// <summary>
        /// Imports a RecordBatch from an FfiCData pointer produced by Rust
        /// and frees the pointer via <c>free_ffi_cdata</c> before returning.
        /// The FfiCData layout is { array: *FFI_ArrowArray, schema: *FFI_ArrowSchema }.
        /// </summary>
        internal static unsafe RecordBatch ImportRecordBatchFromCData(IntPtr ffiCDataPtr)
        {
            try
            {
                var arrayPtr = Marshal.ReadIntPtr(ffiCDataPtr);
                var schemaPtr = Marshal.ReadIntPtr(ffiCDataPtr + IntPtr.Size);
                var schema = CArrowSchemaImporter.ImportSchema((CArrowSchema*)schemaPtr);
                return CArrowArrayImporter.ImportRecordBatch((CArrowArray*)arrayPtr, schema);
            }
            finally
            {
                NativeCall.free_ffi_cdata(ffiCDataPtr);
            }
        }

        /// <summary>
        /// Imports a standalone Arrow Schema from a heap-allocated FFI_ArrowSchema
        /// pointer produced by Rust, and frees the pointer via <c>free_ffi_schema</c>
        /// before returning.
        /// </summary>
        internal static unsafe Schema ImportSchemaFromCData(IntPtr ffiSchemaPtr)
        {
            try
            {
                return CArrowSchemaImporter.ImportSchema((CArrowSchema*)ffiSchemaPtr);
            }
            finally
            {
                NativeCall.free_ffi_schema(ffiSchemaPtr);
            }
        }

        private static ArrayData CloneArrayData(ArrayData data)
        {
            var buffers = new ArrowBuffer[data.Buffers.Length];
            for (int i = 0; i < data.Buffers.Length; i++)
            {
                buffers[i] = CloneBuffer(data.Buffers[i]);
            }
            var childCount = data.Children?.Length ?? 0;
            var children = childCount > 0
                ? new ArrayData[childCount]
                : System.Array.Empty<ArrayData>();
            for (int i = 0; i < childCount; i++)
            {
                children[i] = CloneArrayData(data.Children![i]);
            }
            return new ArrayData(
                data.DataType, data.Length, data.NullCount,
                data.Offset, buffers, children);
        }

        private static ArrowBuffer CloneBuffer(ArrowBuffer buffer)
        {
            if (buffer.Length == 0)
            {
                return ArrowBuffer.Empty;
            }
            // ToArray() copies the buffer bytes into a new managed byte[],
            // which produces an ArrowBuffer with _memoryOwner == null.
            // This enables the pin-based (non-destructive) export path
            // in CArrowArrayExporter.
            return new ArrowBuffer(buffer.Span.ToArray());
        }
    }
}
