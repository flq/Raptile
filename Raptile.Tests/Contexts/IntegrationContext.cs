using OpenFileSystem.IO;
using OpenFileSystem.IO.FileSystems.InMemory;
using Path = System.IO.Path;

namespace Raptile.Tests
{
    public class IntegrationContext
    {
        private string _lastFileName;
        private IObjectStore<string> _lastObjectStore;
        protected IFileSystem Fs { get; private set; }

        public IntegrationContext()
        {
            Defaults.PageItemCount = 1000;
            Fs = new InMemoryFileSystem();
            Raptile.FileSystem = Fs;
        }

        protected IObjectStore<string> ReloadObjectStore()
        {
            _lastObjectStore.Dispose();
            return NewObjectStore(_lastFileName);
        }

        protected IObjectStore<string> NewObjectStore(string fileName = null)
        {
            _lastFileName = fileName ?? Path.GetRandomFileName();
            _lastObjectStore = Raptile.OpenObjectStore<string>(new Settings(_lastFileName) { Serializer = new ServiceStackSerializer() });
            return _lastObjectStore;
        }
    }
}