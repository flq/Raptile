using System;
using System.Text;
using OpenFileSystem.IO;
using OpenFileSystem.IO.FileSystems.InMemory;
using System.Linq;
using Raptile.Storage;

namespace Raptile.Tests
{
    public class StorageFileTestContext
    {
        private readonly InMemoryFileSystem _fs;
        private readonly IFile _write;
        private readonly IFile _rec;
        private StorageFile<string> _storage;

        protected int KeyLength = 10;

        public StorageFileTestContext()
        {
            _fs = new InMemoryFileSystem();
            _write = _fs.GetFile(@"c:\db.mgdat");
            _rec = _fs.GetFile(@"c:\db.mgrec");
            CreateStorage();
        }

        private void CreateStorage()
        {
            _storage = new StorageFile<string>(_write, _rec, KeyLength);
        }

        protected void Store(string key, string value)
        {
            _storage.WriteData(key, Encoding.UTF8.GetBytes(value), false);
        }

        protected string Get(string key)
        {
            try
            {
                var keyValue = _storage.Traverse().First(kv => kv.Key == key);
                return Encoding.UTF8.GetString(keyValue.Value);
            }
            catch (Exception)
            {
                return null;
            }
        }

        protected Action Do(Action action)
        {
            return action;
        }

        protected void ReloadStorage()
        {
            _storage.Shutdown();
            CreateStorage();
        }
    }
}