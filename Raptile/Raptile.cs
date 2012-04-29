using System;
using OpenFileSystem.IO;
using OpenFileSystem.IO.FileSystems.Local.Win32;

namespace Raptile
{
    public static class Raptile
    {

        public static IRaptileDB<T> Open<T>(Settings settings) where T : IComparable<T>
        {
            if (typeof(T).Equals(typeof(string)))
                return (IRaptileDB<T>)new RaptileDBString(Settings.FileSystem, settings);
            if (typeof(T).Equals(typeof(Guid)))
                return (IRaptileDB<T>)new RaptileDBGuid(Settings.FileSystem, settings);
            return new KeyStore<T>(Settings.FileSystem, settings);
        }

        public static IObjectStore<T> OpenObjectStore<T>(Settings settings) where T : IComparable<T>
        {
            var db = Open<T>(settings);
            return new ObjectStore<T>(db, settings);
        }
    }
}
