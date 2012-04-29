using System;
using System.Collections.Generic;
using Raptile.Indices;
using System.Linq;
using System.Linq.Expressions;

namespace Raptile
{
    public interface IObjectStore<K> : IDisposable
    {
        T Get<T>(K key);
        T Get<T>(Expression<Func<object>> query);
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
        private readonly IRaptileInternalDB _dbInternal;
        private readonly ISerializer _serializer;
        private readonly SecondaryIndexContainer _secondaries;

        public ObjectStore(IRaptileDB<K> db, Settings settings)
        {
            _db = db;
            _dbInternal = (IRaptileInternalDB)db;
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

        public T Get<T>(Expression<Func<object>> query)
        {
            var record = _secondaries.Find<T>(query);
            if (record != null)
                return Deserialize<T>(record.Value);
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
            return _secondaries.Enumerate(indexName).Select(Deserialize<T>);
        }

        public void Dispose()
        {
            _secondaries.Dispose();
            _db.Dispose();
        }

        private T Deserialize<T>(int recordNumber)
        {
            return _serializer.Deserialize<T>(_dbInternal.ReadData(recordNumber));
        }
    }
}