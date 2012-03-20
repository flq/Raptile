﻿using System;
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

        private readonly System.Timers.Timer _savetimer;
        private readonly object _lock = new object();

        public KeyStore(IFileSystem fileSystem, Path file) : this(fileSystem, file, Global.DefaultStringKeySize, false)
        {
            
        }

        public KeyStore(IFileSystem fileSystem, Path file, byte maxKeySize, bool allowDuplicateKeys)
        {   
            _maxKeySize = RdbDataType<T>.GetByteSize(maxKeySize);

            var dbFile = file.ChangeExtension(DbFiles.DatExtension);
            var recFile = file.ChangeExtension(DbFiles.RecExtension);

            _index = new MGIndex<T>(fileSystem, file.ChangeExtension(DbFiles.IdxExtension), _maxKeySize, Global.PageItemCount, allowDuplicateKeys);

            _storageFile = new StorageFile<T>(fileSystem.GetFile(dbFile), fileSystem.GetFile(recFile), _maxKeySize);

            _log.Debug("Current Count = " + Count(false).ToString("#,0"));

            CheckIndexState();

            _log.Debug("Starting save timer");
            _savetimer = new System.Timers.Timer();
            _savetimer.Elapsed += HandleSavetimerElapsed;
            _savetimer.Interval = Global.SaveTimerSeconds * 1000;
            _savetimer.AutoReset = true;
            _savetimer.Start();
        }

        public IEnumerable<int> GetDuplicates(T key)
        {
            // get duplicates from index
            return _index.GetDuplicates(key);
        }

        public byte[] FetchRecordBytes(int record)
        {
            return _storageFile.ReadData(record);
        }

        public IEnumerable<KeyValuePair<T, byte[]>> EnumerateStorageFile()
        {
            return _storageFile.Traverse();
        }

        public IEnumerable<KeyValuePair<T, int>> Enumerate(T fromkey)//, bool includeDuplicates, int start, int count)
        {
            lock (_lock)
            {
                // generate a list from the start key using forward only pages
                return _index.Enumerate(fromkey);//, includeDuplicates, start, count);
            }
        }

        public bool RemoveKey(T key)
        {
            // remove and store key in storage file
            _storageFile.WriteData(key, null, true);
            return _index.RemoveKey(key);
        }

        public long Count(bool includeDuplicates)
        {
            return _index.Count(includeDuplicates);
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

        public void Set(T key, byte[] data)
        {
            lock (_lock)
            {
                // save to storage
                var recno = _storageFile.WriteData(key, data, false);
                // save to index
                _index.Set(key, recno);
            }
        }

        public void Shutdown()
        {
            lock (_lock)
            {
                if (_index != null)
                    _log.Debug("Shutting down");
                else
                    return;

                SaveLastRecord();

                _index.Shutdown();
                if (_storageFile != null)
                    _storageFile.Shutdown();
                _index = null;
                _storageFile = null;
                _log.Debug("Shutting down log");
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

        void HandleSavetimerElapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            _index.SaveIndex();
        }

        public void Dispose()
        {
            Shutdown();
        }
    }
}