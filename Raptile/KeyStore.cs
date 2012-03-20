using System;
using System.Collections.Generic;
using OpenFileSystem.IO;
using Raptile.DataTypes;
using Raptile.Indices;
using Raptile.Storage;

namespace Raptile
{
    internal class KeyStore<T> : IRaptileDB<T> where T : IComparable<T>
    {
        private readonly ILog _log = LogManager.GetLogger(typeof(KeyStore<T>));
        private readonly byte _maxKeySize;
        private StorageFile<T> _storageFile;
        private MGIndex<T> _index;

        private readonly System.Timers.Timer _savetimer = new System.Timers.Timer();
        private readonly object _lock = new object();

        public KeyStore(IFileSystem fileSystem, Settings settings)
        {   
            _maxKeySize = RdbDataType<T>.GetByteSize(settings.DefaultStringKeySize);
            var file = settings.DbFileName;

            var dbFile = file.ChangeExtension(DbFiles.DatExtension);
            var recFile = file.ChangeExtension(DbFiles.RecExtension);

            _index = new MGIndex<T>(fileSystem, file.ChangeExtension(DbFiles.IdxExtension), _maxKeySize, Defaults.PageItemCount);

            _storageFile = new StorageFile<T>(fileSystem.GetFile(dbFile), fileSystem.GetFile(recFile), _maxKeySize);

            _log.Debug("Current Count = " + Count.ToString("#,0"));

            CheckIndexState();

            SetupTimer(settings);
        }

        public byte[] FetchRecordBytes(int record)
        {
            return _storageFile.ReadData(record);
        }

        public IEnumerable<KeyValuePair<T, byte[]>> EnumerateStorageFile()
        {
            return _storageFile.Traverse();
        }

        public IEnumerable<KeyValuePair<T, int>> Enumerate(T fromkey)
        {
            lock (_lock)
            {
                // generate a list from the start key using forward only pages
                return _index.Enumerate(fromkey);
            }
        }

        public bool Remove(T key)
        {
            // remove and store key in storage file
            _storageFile.WriteData(key, null, true);
            return _index.RemoveKey(key);
        }

        public long Count
        {
            get { return _index.Count; }
        }

        public bool Get(T key, out byte[] val)
        {
            val = null;
            lock (_lock)
            {
                // search index
                int recordNumber;
                if (_index.Get(key, out recordNumber))
                {
                    val = _storageFile.ReadData(recordNumber);
                    return true;
                }
                return false;
            }
        }

        public int Set(T key, byte[] data)
        {
            lock (_lock)
            {
                // save to storage
                var recno = _storageFile.WriteData(key, data, false);
                // save to index
                _index.Set(key, recno);
                return recno;
            }
        }

        public void Dispose()
        {
            lock (_lock)
            {
                _log.Info("shutting down key Store...");

                SaveLastRecord();

                _index.Dispose();
                _storageFile.Dispose();
                _index = null;
                _storageFile = null;
            }
        }

        public Statistics GetStatistics()
        {
            return _index.GetStatistics();
        }

        private void SaveLastRecord()
        {
            // save the last record number in the index file
            _index.SaveLastRecordNumber(_storageFile.Count());
        }

        private void CheckIndexState()
        {
            _log.Debug("Checking Index state...");
            int last = _index.GetLastIndexedRecordNumber();
            int count = _storageFile.Count();
            if (last < count)
            {
                _log.Debug("Rebuilding index...");
                _log.Debug("   last index count = " + last);
                _log.Debug("   data items count = " + count);
                // check last index record and archive record
                //       rebuild index if needed
                for (int i = last; i < count; i++)
                {
                    bool deleted;
                    T key = _storageFile.GetKey(i, out deleted);
                    if (deleted == false)
                        _index.Set(key, i);
                    else
                        _index.RemoveKey(key);

                    if (i % 100000 == 0)
                        _log.Debug("100,000 items re-indexed");
                }
                _log.Debug("Rebuild index done.");
            }
        }

        private void SetupTimer(Settings settings)
        {
            _log.Debug("Starting save timer");
            _savetimer.Elapsed += HandleSavetimerElapsed;
            _savetimer.Interval = settings.AutoSaveTimespan.TotalMilliseconds;
            _savetimer.AutoReset = true;
            _savetimer.Start();
        }

        void HandleSavetimerElapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            _index.SaveIndex();
        }
    }
}
