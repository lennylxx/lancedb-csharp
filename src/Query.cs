namespace lancedb
{
    using System.Runtime.InteropServices;

    /// <summary>
    /// A builder for LanceDB queries.
    /// </summary>
    /// <remarks>
    /// This class is not intended to be created directly. Instead, use the
    /// <see cref="Table.Query"/> method to create a query.
    /// </remarks>
    public class Query : QueryBase
    {
        [DllImport(NativeLibrary.Name, CallingConvention = CallingConvention.Cdecl)]
        private static extern IntPtr query_nearest_to(IntPtr query_ptr, double[] vector, UIntPtr len);

        internal Query(IntPtr queryPtr)
            : base(queryPtr)
        {
        }

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
            IntPtr vectorQueryPtr = query_nearest_to(QueryPtr, vector, (UIntPtr)vector.Length);
            return new VectorQuery(vectorQueryPtr);
        }
    }
}