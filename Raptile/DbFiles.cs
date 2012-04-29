using OpenFileSystem.IO;

namespace Raptile
{
    internal static class DbFiles
    {
        public const string DatExtension = ".mgdat";
        public const string IdxExtension = ".mgidx";
        public const string RecExtension = ".mgrec";
        public const string BitmapRecExtension = ".mgbmr";
        public const string BitmapExtension = ".mgbmp";
        public const string SecondaryIndexExtension = ".secidx";

        public static Path ChangeExtension(this Path file, string extension)
        {
            var path = file.DirectoryName;
            var fullFileName = System.IO.Path.GetFileNameWithoutExtension(file) + extension;
            return !string.IsNullOrEmpty(path) ? new Path(path).Combine(fullFileName) : new Path(fullFileName);
        }

        public static Path NewFileInThisDir(this Path file, string name)
        {
            var path = file.DirectoryName;
            if (string.IsNullOrEmpty(path))
                return new Path(name);
            return new Path(path).Combine(name);
        }
    }
}