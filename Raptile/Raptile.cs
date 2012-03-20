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

        public static IRaptileDB<T> Open<T>(string filename) where T : IComparable<T>
        {
            if (typeof(T).Equals(typeof(string)))
                return (IRaptileDB<T>)new RaptileDBString(FileSystem, filename, true);
            if (typeof(T).Equals(typeof(Guid)))
                return (IRaptileDB<T>)new RaptileDBGuid(FileSystem, filename);
            return new KeyStore<T>(FileSystem, new Path(filename));
        }

        public static IObjectStore<T> Open<T>(string filename, ISerializer serializer) where T : IComparable<T>
        {
            var db = Open<T>(filename);
            return new ObjectStore<T>(db, serializer);
        }
    }
}
