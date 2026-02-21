namespace lancedb
{
    using System.Runtime.InteropServices;

    /// <summary>
    /// A builder for LanceDB queries.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This class is not intended to be created directly. Instead, use the
    /// <see cref="Table.Query"/> method to create a query.
    /// </para>
    /// <para>
    /// Queries allow you to search your existing data. By default the query will
    /// return all the data in the table in no particular order. The builder
    /// methods can be used to control the query using filtering, projection,
    /// and limits.
    /// </para>
    /// </remarks>
    public class Query : QueryBase<Query>
    {
        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr query_nearest_to(IntPtr query_ptr, double[] vector, UIntPtr len);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void query_free(IntPtr query_ptr);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr query_select(IntPtr query_ptr, IntPtr columns_json);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr query_only_if(IntPtr query_ptr, IntPtr predicate);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr query_limit(IntPtr query_ptr, ulong limit);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr query_offset(IntPtr query_ptr, ulong offset);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr query_with_row_id(IntPtr query_ptr);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr query_full_text_search(IntPtr query_ptr, IntPtr query_text);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr query_fast_search(IntPtr query_ptr);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr query_postfilter(IntPtr query_ptr);

        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern void query_execute(IntPtr query_ptr, NativeCall.FfiCallback completion);

        internal Query(IntPtr queryPtr)
            : base(queryPtr)
        {
        }

        /// <inheritdoc/>
        protected override void NativeFree(IntPtr ptr) => query_free(ptr);

        /// <inheritdoc/>
        protected override IntPtr NativeSelect(IntPtr ptr, IntPtr columnsJson)
            => query_select(ptr, columnsJson);

        /// <inheritdoc/>
        protected override IntPtr NativeOnlyIf(IntPtr ptr, IntPtr predicate)
            => query_only_if(ptr, predicate);

        /// <inheritdoc/>
        protected override IntPtr NativeLimit(IntPtr ptr, ulong limit)
            => query_limit(ptr, limit);

        /// <inheritdoc/>
        protected override IntPtr NativeOffset(IntPtr ptr, ulong offset)
            => query_offset(ptr, offset);

        /// <inheritdoc/>
        protected override IntPtr NativeWithRowId(IntPtr ptr)
            => query_with_row_id(ptr);

        /// <inheritdoc/>
        protected override IntPtr NativeFullTextSearch(IntPtr ptr, IntPtr queryText)
            => query_full_text_search(ptr, queryText);

        /// <inheritdoc/>
        protected override IntPtr NativeFastSearch(IntPtr ptr)
            => query_fast_search(ptr);

        /// <inheritdoc/>
        protected override IntPtr NativePostfilter(IntPtr ptr)
            => query_postfilter(ptr);

        /// <inheritdoc/>
        private protected override void NativeExecute(IntPtr ptr, NativeCall.FfiCallback callback)
            => query_execute(ptr, callback);

        /// <summary>
        /// Find the nearest vectors to the given query vector.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This converts the query from a plain query to a vector query.
        /// </para>
        /// <para>
        /// The input should be an array of floats (or doubles) that represents the
        /// query vector.
        /// </para>
        /// <para>
        /// If there is only one vector column (a column whose data type is a
        /// fixed size list of floats) then the column does not need to be specified.
        /// If there is more than one vector column you must use
        /// <see cref="VectorQuery.Column"/> to specify which column to compare with.
        /// </para>
        /// <para>
        /// If no index has been created on the vector column then a vector query
        /// will perform a distance comparison between the query vector and every
        /// vector in the database and then sort the results. This is sometimes
        /// called a "flat search".
        /// </para>
        /// <para>
        /// For small databases, with tens of thousands of vectors or less, this can
        /// be reasonably fast. In larger databases you should create a vector index
        /// on the column. If there is a vector index then an "approximate" nearest
        /// neighbor search (frequently called an ANN search) will be performed. This
        /// search is much faster, but the results will be approximate.
        /// </para>
        /// <para>
        /// Vector searches always have a limit. If <c>Limit</c> has not been called then
        /// a default limit of 10 will be used.
        /// </para>
        /// </remarks>
        /// <param name="vector">The query vector to search for nearest neighbors.</param>
        /// <returns>A <see cref="VectorQuery"/> that can be used to further parameterize the search.</returns>
        public VectorQuery NearestTo(double[] vector)
        {
            IntPtr vectorQueryPtr = query_nearest_to(NativePtr, vector, (UIntPtr)vector.Length);
            return new VectorQuery(vectorQueryPtr);
        }
    }
}