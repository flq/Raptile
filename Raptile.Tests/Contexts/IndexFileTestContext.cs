using System;
using OpenFileSystem.IO;
using OpenFileSystem.IO.FileSystems.InMemory;
using Raptile.Indices;

namespace Raptile.Tests
{
    public class IndexFileTestContext
    {
        private readonly InMemoryFileSystem _fs;

        protected byte KeyLength = 10;
        private MGIndex<string> _index;

        public IndexFileTestContext()
        {
            _fs = new InMemoryFileSystem();
            CreateIndex();
        }

        private void CreateIndex()
        {
            _index = new MGIndex<string>(_fs, new Path(@"c:\index.mgidx"), KeyLength, 300);
        }

        protected void SetKeyForRecordNumber(string key, int recordNumber)
        {
            _index.Set(key, recordNumber);
        }

        protected int Get(string key)
        {
            int recordNumber;
            if (_index.Get(key, out recordNumber))
                return recordNumber;
            return -1;
        }

        protected Action Do(Action action)
        {
            return action;
        }

        protected void ReloadIndex()
        {
            _index.Shutdown();
            CreateIndex();
        }
    }
}