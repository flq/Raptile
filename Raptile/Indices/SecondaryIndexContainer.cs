using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Raptile.Indices
{
    public class SecondaryIndexContainer : ISecondaryIndex
    {
        readonly IDictionary<string, ISecondaryIndex> _indices = new Dictionary<string, ISecondaryIndex>();

        public void AddIndex(ISecondaryIndex index)
        {
            _indices.Add(index.IndexName, index);
        }

        IEnumerator<int> IEnumerable<int>.GetEnumerator()
        {
            return _indices.SelectMany(sec => sec.Value).Distinct().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return _indices.SelectMany(sec => sec.Value).Distinct().GetEnumerator();
        }

        public void Dispose()
        {
            foreach (var sec in _indices.Values)
              sec.Dispose();
        }

        public void Inspect(int recordNumber, object obj)
        {
            foreach (var sec in _indices.Values)
                sec.Inspect(recordNumber, obj);
        }

        public void Remove(int recordNumber)
        {
            foreach (var sec in _indices.Values)
                sec.Remove(recordNumber);
        }

        public string IndexName
        {
            get { return "Contains " + string.Join(", ", _indices.Keys); }
        }

        public bool SupportsSearch
        {
            get { return _indices.Values.Any(idx => idx.SupportsSearch); }
        }

        public int? Find<T>(Expression<Func<object>> query)
        {
            return _indices.Values
                .Where(idx => idx.SupportsSearch)
                .Select(idx => idx.Find<T>(query))
                .FirstOrDefault(i => i.HasValue);
        }

        public IEnumerable<int> Enumerate(string indexName)
        {
            ISecondaryIndex idx;
            return _indices.TryGetValue(indexName, out idx) ? idx : Enumerable.Empty<int>();
        }
    }
}