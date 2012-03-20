using OpenFileSystem.IO;
using OpenFileSystem.IO.FileSystems.InMemory;
using Path = System.IO.Path;

namespace Raptile.Tests
{
    public class IntegrationContext
    {
        protected IFileSystem Fs { get; private set; }

        public IntegrationContext()
        {
            Global.PageItemCount = 1000;
            Fs = new InMemoryFileSystem();
            Raptile.FileSystem = Fs;
        }

        protected IObjectStore<string> NewObjectStore()
        {
            var f = Path.GetRandomFileName();
            return Raptile.Open<string>(f, new ServiceStackSerializer());
        }
    }
}