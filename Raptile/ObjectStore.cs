using System;
using System.Collections.Generic;
using Raptile.Indices;
using System.Linq;

namespace Raptile
{
    public interface IObjectStore<K> : IDisposable
    {
        T Get<T>(K key);
        void Set(K key, object obj);
        long Count { get; }
        IEnumerable<T> Enumerate<T>(string indexName);
    }

    public interface ISerializer
    {
        byte[] Serialize(object obj);
        T Deserialize<T>(byte[] contents);
    }

    public class ObjectStore<K> : IObjectStore<K> where K : IComparable<K>
    {
        private readonly IRaptileDB<K> _db;
        private readonly ISerializer _serializer;
        private readonly SecondaryIndexContainer _secondaries;

        public ObjectStore(IRaptileDB<K> db, Settings settings)
        {
            _db = db;
            _serializer = settings.Serializer;
            _secondaries = settings.Indices;
        }

        public T Get<T>(K key)
        {
            byte[] results;
            if (_db.Get(key, out results))
                return _serializer.Deserialize<T>(results);
            return default(T);
        }

        public void Set(K key, object obj)
        {
            var bytes = _serializer.Serialize(obj);
            var recno = _db.Set(key, bytes);
            _secondaries.Inspect(recno, obj);
        }

        public long Count
        {
            get { return _db.Count; }
        }

        public IEnumerable<T> Enumerate<T>(string indexName)
        {
            var db = (IRaptileInternalDB)_db;
            return _secondaries.Enumerate(indexName).Select(db.ReadData).Select(_serializer.Deserialize<T>);
        }

        public void Dispose()
        {
            _secondaries.Dispose();
            _db.Dispose();
        }
    }
}