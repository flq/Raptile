using System;
using System.Collections.Generic;
using System.IO;
using OpenFileSystem.IO;
using Path = OpenFileSystem.IO.Path;

namespace Raptile.Indices
{
    internal class BitmapIndex
    {
        private readonly SafeDictionary<int, long> _offsetCache = new SafeDictionary<int, long>();
        private readonly ILog log = LogManager.GetLogger(typeof(BitmapIndex));
        private Stream _bitmapFileWrite;
        private Stream _bitmapFileRead;
        private Stream _recordFileRead;
        private Stream _recordFileWrite;
        private SafeDictionary<int, WAHBitArray> _cache = new SafeDictionary<int, WAHBitArray>();
        private int _lastRecordNumber;
        private long _lastBitmapOffset;

        public BitmapIndex(IFileSystem fs, Path file)
        {
            var rec = file.ChangeExtension(DbFiles.BitmapRecExtension);
            var bmp = file.ChangeExtension(DbFiles.BitmapExtension);

            _recordFileRead = fs.GetFile(rec).Open(FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            _recordFileWrite = fs.GetFile(rec).Open(FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            _bitmapFileWrite = fs.GetFile(bmp).Open(FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            _bitmapFileRead = fs.GetFile(bmp).Open(FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);

            _bitmapFileWrite.Seek(0L, SeekOrigin.End);
            _lastBitmapOffset = _bitmapFileWrite.Length;
            _lastRecordNumber = (int)(_recordFileRead.Length / 8);
        }

        public void Shutdown()
        {
            log.Debug("Shutdown BitmapIndex");
            Flush();
            if (_recordFileRead != null)
            {
                _recordFileRead.Close();
                _recordFileWrite.Close();
                _bitmapFileWrite.Close();
                _bitmapFileRead.Close();
                _recordFileRead = null;
                _recordFileWrite = null;
                _bitmapFileRead = null;
                _bitmapFileWrite = null;
            }
        }

        public void Flush()
        {
            if (_recordFileWrite != null)
                _recordFileWrite.Flush();
            if (_bitmapFileWrite != null)
                _bitmapFileWrite.Flush();
        }

        public int GetFreeRecordNumber()
        {
            int i = _lastRecordNumber++;

            _cache.Add(i, new WAHBitArray());
            return i;
        }

        public void Commit(bool freeMemory)
        {
            foreach (KeyValuePair<int, WAHBitArray> kv in _cache)
            {
                if (kv.Value.isDirty)
                {
                    SaveBitmap(kv.Key, kv.Value);
                    kv.Value.FreeMemory();
                    kv.Value.isDirty = false;
                }
            }
            Flush();
            if (freeMemory)
            {
                _cache = new SafeDictionary<int, WAHBitArray>();
            }
        }

        public void SetDuplicate(int bitmaprecno, int record)
        {
            var ba = GetBitmap(bitmaprecno);
            ba.Set(record, true);
        }

        public WAHBitArray GetBitmap(int recno)
        {
            return internalGetBitmap(recno, true);
        }

        public WAHBitArray GetBitmapNoCache(int recno)
        {
            return internalGetBitmap(recno, false);
        }

        private WAHBitArray internalGetBitmap(int recno, bool usecache)
        {
            WAHBitArray ba;

            if (_cache.TryGetValue(recno, out ba))
                return ba;

            long offset;
            if (_offsetCache.TryGetValue(recno, out offset) == false)
            {
                byte[] b = new byte[8];
                long off = ((long)recno) * 8;
                _recordFileRead.Seek(off, SeekOrigin.Begin);
                _recordFileRead.Read(b, 0, 8);
                offset = Converter.ToInt64(b, 0);
                _offsetCache.Add(recno, offset);
            }
            ba = LoadBitmap(offset);
            if (usecache)
                _cache.Add(recno, ba);

            return ba;
        }

        private void SaveBitmap(int recno, WAHBitArray bmp)
        {
            long offset = SaveBitmapToFile(bmp);
            long v;
            if (_offsetCache.TryGetValue(recno, out v))
                _offsetCache[recno] = offset;
            else
                _offsetCache.Add(recno, offset);

            long pointer = ((long)recno) * 8;
            _recordFileWrite.Seek(pointer, SeekOrigin.Begin);
            byte[] b = new byte[8];
            b = Converter.GetBytes(offset, false);
            _recordFileWrite.Write(b, 0, 8);
        }

        //-----------------------------------------------------------------
        // BITMAP FILE FORMAT
        //    0  'B','M'
        //    2  uint count = 4 bytes
        //    6  Bitmap type    0 = int record list      1 = uint bitmap
        //    7  '0'
        //    8  uint data
        //-----------------------------------------------------------------
        private long SaveBitmapToFile(WAHBitArray bmp)
        {
            var off = _lastBitmapOffset;

            var bits = bmp.GetCompressed();

            var b = new byte[bits.Length * 4 + 8];
            // write header data
            b[0] = ((byte)'B');
            b[1] = ((byte)'M');
            Buffer.BlockCopy(Converter.GetBytes(bits.Length, false), 0, b, 2, 4);
            b[6] = (byte)(bmp.UsingIndexes ? 0 : 1);
            b[7] = 0;

            for (int i = 0; i < bits.Length; i++)
            {
                byte[] u = Converter.GetBytes((int)bits[i], false);
                Buffer.BlockCopy(u, 0, b, i * 4 + 8, 4);
            }
            _bitmapFileWrite.Write(b, 0, b.Length);
            _lastBitmapOffset += b.Length;
            return off;
        }

        private WAHBitArray LoadBitmap(long offset)
        {
            var bc = new WAHBitArray();
            if (offset == -1)
                return bc;

            var ar = new List<uint>();
            WAHBitArray.WAHType wahType = WAHBitArray.WAHType.Compressed;
            {
                _bitmapFileRead.Seek(offset, SeekOrigin.Begin);

                byte[] b = new byte[8];

                _bitmapFileRead.Read(b, 0, 8);
                if (b[0] == (byte)'B' && b[1] == (byte)'M' && b[7] == 0)
                {
                    wahType = (b[6] == 0 ? WAHBitArray.WAHType.Indexes : WAHBitArray.WAHType.Compressed);
                    int c = Converter.ToInt32(b, 2);
                    byte[] buf = new byte[c * 4];
                    _bitmapFileRead.Read(buf, 0, c * 4);
                    for (int i = 0; i < c; i++)
                    {
                        ar.Add((uint)Converter.ToInt32(buf, i * 4));
                    }
                }
            }
            bc = new WAHBitArray(wahType, ar.ToArray());

            return bc;
        }
    }
}
