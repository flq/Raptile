using System;
using Raptile.Indices;
using Path = OpenFileSystem.IO.Path;

namespace Raptile
{
    internal class Defaults
    {
        public static ushort PageItemCount = 10000;
        public static int SaveTimerSeconds = 300;
        public static byte DefaultStringKeySize = 60;
        public static bool FlushStorageFileImmediatly;
    }

    public class Settings
    {
        private ISerializer _serializer;

        private readonly SecondaryIndexContainer _indices = new SecondaryIndexContainer();

        public Settings(string dbFilename)
        {
            DbFileName = new Path(dbFilename);
            PageItemCount = Defaults.PageItemCount;
            AutoSaveTimespan = TimeSpan.FromSeconds(Defaults.SaveTimerSeconds);
            DefaultStringKeySize = Defaults.DefaultStringKeySize;
            FlushStorageFileImmediatly = Defaults.FlushStorageFileImmediatly;
        }

        public Path DbFileName { get; private set; }
        public ushort PageItemCount { get; set; }
        public TimeSpan AutoSaveTimespan { get; set; }
        public byte DefaultStringKeySize { get; set; }
        public bool FlushStorageFileImmediatly { get; set; }

        public ISerializer Serializer
        {
            get { return _serializer?? (_serializer = new DefaultSerializer()); }
            set { _serializer = value; }
        }

        public SecondaryIndexContainer Indices
        {
            get { return _indices; }
        }

        public void AddIndex<T>(string indexName, Func<T, bool> indexer)
        {
            var secondaryIndex = new SecondaryIndex<T>(indexName, indexer);
            secondaryIndex.SetUp(Raptile.FileSystem, this);
            _indices.AddIndex(secondaryIndex);
        }
    }
}
