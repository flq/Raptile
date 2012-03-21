using System;
using System.Collections.Generic;
using System.Linq;
using OpenFileSystem.IO;
using OpenFileSystem.IO.FileSystems.InMemory;
using Raptile.Indices;

namespace Raptile.Tests
{
    public class SecondaryIndexContext
    {
        private readonly IFileSystem _fs = new InMemoryFileSystem();
        private Func<ISecondaryIndex> _secIndexBuilder;

        protected void LoadIndex(string name, Func<object,bool> indexer)
        {
            _secIndexBuilder = () => new SecondaryIndex(name, indexer).SetUp(Fs, new Settings(@"c:\db\."));
            Index = new SecondaryIndex(name, indexer).SetUp(Fs, new Settings(@"c:\db\."));
        }

        protected void LoadIndex<T>(string name, Func<T, bool> indexer)
        {
            _secIndexBuilder = () => new SecondaryIndex<T>(name, indexer).SetUp(Fs, new Settings(@"c:\db\."));
            Index = _secIndexBuilder();
        }

        protected void ReloadIndex()
        {
            if (_secIndexBuilder == null)
                throw new InvalidOperationException("Reload only supported if Index was previously set up");
            Index.Dispose();
            Index = _secIndexBuilder();
        }


        protected void ToIndex(int recordNumber, object o)
        {
            Index.Inspect(recordNumber, o);
        }

        protected void Remove(int recordNumber)
        {
            Index.Remove(recordNumber);
        }

        protected IFileSystem Fs { get { return _fs; } }

        protected ISecondaryIndex Index { get; private set; }

        protected List<int> IndexContents { get { return Index.ToList(); } }
    }
}