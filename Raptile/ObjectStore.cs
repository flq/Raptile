using System;

namespace Raptile
{
    public interface IObjectStore<K> : IDisposable
    {
        T Get<T>(K key);
        void Set(K key, object obj);
        long Count { get; }
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

        public ObjectStore(IRaptileDB<K> db, ISerializer serializer)
        {
            _db = db;
            _serializer = serializer;
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
            _db.Set(key, bytes);
        }

        public long Count
        {
            get { return _db.Count; }
        }

        public void Dispose()
        {
            _db.Dispose();
        }
    }
}