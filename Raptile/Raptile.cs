using System;
using OpenFileSystem.IO;
using OpenFileSystem.IO.FileSystems.Local.Win32;

namespace Raptile
{
    public static class Raptile
    {
        private static IFileSystem fileSystem = new Win32FileSystem();
        
        public static IFileSystem FileSystem
        {
            get { return fileSystem; }
            set { fileSystem = value; }
        }

        public static IRaptileDB<T> Open<T>(Settings settings) where T : IComparable<T>
        {
            if (typeof(T).Equals(typeof(string)))
                return (IRaptileDB<T>)new RaptileDBString(FileSystem, settings);
            if (typeof(T).Equals(typeof(Guid)))
                return (IRaptileDB<T>)new RaptileDBGuid(FileSystem, settings);
            return new KeyStore<T>(FileSystem, settings);
        }

        public static IObjectStore<T> OpenObjectStore<T>(Settings settings) where T : IComparable<T>
        {
            var db = Open<T>(settings);
            return new ObjectStore<T>(db, settings);
        }
    }
}
