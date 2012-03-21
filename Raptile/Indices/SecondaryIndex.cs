using System;
using System.Collections;
using System.Collections.Generic;
using OpenFileSystem.IO;
using ProtoBuf;

namespace Raptile.Indices
{
    public interface ISecondaryIndex : IEnumerable<int>, IDisposable
    {
        void Inspect(int recordNumber, object obj);
        void Remove(int recordNumber);
    }

    public class SecondaryIndex<T> : SecondaryIndex
    {
        /// <summary>
        /// Use if you just want an index on type
        /// </summary>
        public SecondaryIndex(string indexName) : this(indexName, _ => true)
        {
            
        }

        public SecondaryIndex(string indexName, Func<T, bool> predicate) : base(indexName, o => o is T && predicate((T)o))
        {
        }
    }

    public class SecondaryIndex : ISecondaryIndex
    {
        private readonly string _indexName;
        private readonly Func<object, bool> _predicate;
        private SecondaryIndexStorage _storage = new SecondaryIndexStorage();
        private IFile _file;

        [ProtoContract]
        private class SecondaryIndexStorage
        {
            public SecondaryIndexStorage()
            {
                Records = new HashSet<int>();
            }
            
            [ProtoMember(1)]
            public HashSet<int> Records { get; set; }

            public void Add(int recordNumber)
            {
                Records.Add(recordNumber);
            }

            public void Remove(int recordNumber)
            {
                Records.Remove(recordNumber);
            }
        }

        public SecondaryIndex(string indexName, Func<object, bool> predicate)
        {
            _indexName = indexName;
            _predicate = predicate;
        }

        public void Inspect(int recordNumber, object obj)
        {
            if (_predicate(obj))
                _storage.Add(recordNumber);
        }

        public void Remove(int recordNumber)
        {
            _storage.Remove(recordNumber);
        }

        public IEnumerator<int> GetEnumerator()
        {
            return _storage.Records.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public ISecondaryIndex SetUp(IFileSystem fs, Settings settings)
        {
            var directory = settings.DbFileName.DirectoryName;
            var fName = Converter.MurMur.Hash(_indexName) + DbFiles.SecondaryIndexExtension;
            _file = fs.GetDirectory(directory).GetFile(fName);
            LoadIfExists();
            return this;
        }

        private void LoadIfExists()
        {
            if (!_file.Exists)
                return;
            using (var s = _file.OpenRead())
            {
                _storage = Serializer.Deserialize<SecondaryIndexStorage>(s);
            }
        }

        public void Dispose()
        {
            using (var s = _file.OpenWrite())
            {
                Serializer.Serialize(s, _storage);
            }
        }
    }
}