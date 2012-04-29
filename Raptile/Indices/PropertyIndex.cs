using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using OpenFileSystem.IO;

namespace Raptile.Indices
{
    public class PropertyIndex<T> : ISecondaryIndex
    {
        private readonly MGIndex<int> _index;
        private readonly IndexExtractor<T> _extractor;
        private readonly MurmurHash2Unsafe _hasher = new MurmurHash2Unsafe();

        public PropertyIndex(IFileSystem fs, Path path, string name, Expression<Func<T, object>> indexer)
        {
            IndexName = name;
            _index = new MGIndex<int>(fs, path.NewFileInThisDir(name + ".secidx"), Defaults.DefaultStringKeySize, Defaults.PageItemCount);
            _extractor = new IndexExtractor<T>(indexer);
        }

        public IEnumerator<int> GetEnumerator()
        {
            return _index.Enumerate().Select(kv => kv.Value).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Dispose()
        {
            _index.Dispose();
        }

        public void Inspect(int recordNumber, object obj)
        {
            var key = _extractor.Read(obj);
            if (key == null)
                return;

            _index.Set(_hasher.Hash(key), recordNumber);
        }

        public void Remove(int recordNumber)
        {
            var keyAndRecord = _index.Enumerate().FirstOrDefault(kv => kv.Value == recordNumber);
            _index.RemoveKey(keyAndRecord.Key);
        }

        public string IndexName { get; private set; }

        public bool SupportsSearch { get { return true; } }

        public int? Find<O>(Expression<Func<object>> query)
        {
            if (!typeof(T).IsAssignableFrom(typeof(O)))
                return null;
            var key = query.Compile()().ToString();
            var intKey = _hasher.Hash(key);
            int recordNo;
            if (_index.Get(intKey, out recordNo))
                return recordNo;
            return null;
        }
    }
}
