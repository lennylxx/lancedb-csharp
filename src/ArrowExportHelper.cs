namespace lancedb
{
    using Apache.Arrow;
    using Apache.Arrow.C;

    /// <summary>
    /// Helpers for exporting Arrow data via the C Data Interface.
    /// </summary>
    internal static class ArrowExportHelper
    {
        /// <summary>
        /// Enables exporting managed (GC heap) memory through the Arrow C Data Interface.
        /// Must be set before any export calls.
        /// </summary>
        static ArrowExportHelper()
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
