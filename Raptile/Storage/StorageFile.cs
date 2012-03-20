using System;
using System.IO;
using System.Collections.Generic;
using System.Threading;
using OpenFileSystem.IO;
using Raptile.DataTypes;

namespace Raptile.Storage
{
    internal class StorageFile<T> : IDisposable
    {
        private readonly IGetBytes<T> _byteReader;
        Stream _writefile;
        Stream _recordfile;
        Stream _readData;
        Stream _readRec;
        int _lastRecordNum;
        long _lastWriteOffset;
        private bool _flushNeeded;

        public static readonly byte[] Fileheader = 
        { 
            (byte)'M', (byte)'G', (byte)'D', (byte)'B',
            0, // -- [flags] = [shutdownOK:1],
            0  // -- [maxkeylen] 
        };

        public static readonly byte[] Rowheader = 
        { 
            (byte)'M', (byte)'G', (byte)'R' ,
            0,               // 3     [keylen]
            0,0,0,0,0,0,0,0, // 4-11  [datetime] 8 bytes = insert time
            0,0,0,0,         // 12-15 [data length] 4 bytes
            0,               // 16 -- [flags] = 1 : isDeletd:1
                             //                 2 : isCompressed:1
                             //                 
                             //                 
            0                // 17 -- [crc] = header crc check
        };
        private enum HdrPos
        {
            KeyLen = 3,
            DataLength = 12,
            Flags = 16,
            CRC = 17
        }

        public StorageFile(IFile writeFile, IFile recordFile, int maximumKeyLength)
        {
            _byteReader = RdbDataType<T>.ByteHandler();
            _writefile = writeFile.Open(FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
            _recordfile = recordFile.Open(FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
            _readData = writeFile.OpenRead();
            _readRec = recordFile.OpenRead();
            
            if (_writefile.Length == 0)
            {
                // new file
                var b = (byte)maximumKeyLength;
                Fileheader[5] = b;
                _writefile.Write(Fileheader, 0, Fileheader.Length);
                _writefile.Flush();
            }

            _lastRecordNum = (int)(_recordfile.Length / 8);
            _recordfile.Seek(0L, SeekOrigin.End);
            _lastWriteOffset = _writefile.Seek(0L, SeekOrigin.End);
        }

        public int Count()
        {
            return (int)(_recordfile.Length >> 3);
        }

        public IEnumerable<KeyValuePair<T, byte[]>> Traverse()
        {
            long offset = Fileheader.Length;

            while (offset < _writefile.Length)
            {
                var pointer = offset;
                byte[] key;
                bool deleted;
                offset = NextOffset(offset, out key, out deleted);
                var kv = new KeyValuePair<T, byte[]>(_byteReader.GetObject(key, 0, key.Length), ReadDataFile(pointer));
                if (deleted == false)
                    yield return kv;
            }
        }

        public int WriteData(T key, byte[] data, bool deleted)
        {
            byte[] k = _byteReader.GetBytes(key);
            int keyLength = k.Length;

            // seek end of file
            long offset = _lastWriteOffset;
            byte[] header = CreateRowHeader(keyLength, (data==null?0:data.Length));
            if (deleted)
                header[(int)HdrPos.Flags] = 1;
            // write header info
            _writefile.Write(header, 0, header.Length);
            // write key
            _writefile.Write(k, 0, keyLength);
            if (data != null)
            {
                // write data block
                _writefile.Write(data, 0, data.Length);
                if (Defaults.FlushStorageFileImmediatly)
                    _writefile.Flush();
                _lastWriteOffset += data.Length;
            }
            // update pointer
            Interlocked.Add(ref _lastWriteOffset, header.Length);
            Interlocked.Add(ref _lastWriteOffset, keyLength);
            var recno = _lastRecordNum;
            Interlocked.Increment(ref _lastRecordNum);
            _recordfile.Write(Converter.GetBytes(offset, false), 0, 8);
            if (Defaults.FlushStorageFileImmediatly)
                _recordfile.Flush();
            _flushNeeded = true;
            return recno;
        }

        private long NextOffset(long curroffset, out byte[] key, out bool isdeleted)
        {
            isdeleted = false;
            long next = _readData.Length;
            // seek offset in file
            var hdr = new byte[Rowheader.Length];
            _readData.Seek(curroffset, SeekOrigin.Begin);
            // read header
            _readData.Read(hdr, 0, Rowheader.Length);
            key = new byte[hdr[(int) HdrPos.KeyLen]];
            _readData.Read(key, 0, hdr[(int) HdrPos.KeyLen]);
            // check header
            if (CheckHeader(hdr))
            {
                next = curroffset + hdr.Length + Converter.ToInt32(hdr, (int) HdrPos.DataLength) +
                       hdr[(int) HdrPos.KeyLen];
                isdeleted = IsDeleted(hdr);
            }
            return next;
        }

        private static byte[] CreateRowHeader(int keylen, int datalen)
        {
            var rh = new byte[Rowheader.Length];
            Buffer.BlockCopy(Rowheader, 0, rh, 0, rh.Length);
            rh[3] = (byte)keylen;
            Buffer.BlockCopy(Converter.GetBytes(datalen, false), 0, rh, 12,4);

            return rh;
        }

        
        public byte[] ReadData(int recnum)
        {
            if (_flushNeeded)
            {
                _writefile.Flush();
                _recordfile.Flush();
                _flushNeeded = false;
            }
            long dataOffset = recnum * 8;

            var b = new byte[8];

            _readRec.Seek(dataOffset, SeekOrigin.Begin);
            _readRec.Read(b, 0, 8);
            dataOffset = Converter.ToInt64(b, 0);

            return ReadDataFile(dataOffset);
        }

        
        private byte[] ReadDataFile(long offset)
        {
            // seek offset in file
            var hdr = new byte[Rowheader.Length];
            _readData.Seek(offset, SeekOrigin.Begin);
            // read header
            _readData.Read(hdr, 0, Rowheader.Length);
            // check header
            CheckHeader(hdr);
            
            // skip key bytes
            _readData.Seek(hdr[(int) HdrPos.KeyLen], SeekOrigin.Current);
            var dl = Converter.ToInt32(hdr, (int) HdrPos.DataLength);
            var data = new byte[dl];
            // read data block
            _readData.Read(data, 0, dl);
            return data;
        }

        public void Dispose()
        {
            FlushClose(_readData);
            FlushClose(_readRec);
            FlushClose(_recordfile);
            FlushClose(_writefile);

            _readData = null;
            _readRec = null;
            _recordfile = null;
            _writefile = null;
        }

        internal T GetKey(int recnum, out bool deleted)
        {
            deleted = false;
            long off = recnum * 8;

            var b = new byte[8];

            _readRec.Seek(off, SeekOrigin.Begin);
            _readRec.Read(b, 0, 8);
            off = Converter.ToInt64(b, 0);

            // seek offset in file
            var hdr = new byte[Rowheader.Length];
            _readData.Seek(off, SeekOrigin.Begin);
            // read header
            _readData.Read(hdr, 0, Rowheader.Length);

            if (CheckHeader(hdr))
            {
                deleted = IsDeleted(hdr);
                byte kl = hdr[3];
                var kbyte = new byte[kl];

                _readData.Read(kbyte, 0, kl);
                return _byteReader.GetObject(kbyte, 0, kl);
            }

            return default(T);
        }

        private static bool CheckHeader(byte[] hdr)
        {
            if (hdr[0] == (byte)'M' && hdr[1] == (byte)'G' && hdr[2] == (byte)'R' && hdr[(int)HdrPos.CRC] == 0)
                return true;
            throw new InvalidOperationException("Data Header error");
        }

        private static void FlushClose(Stream st)
        {
            if (st == null) return;
            st.Flush();
            st.Close();
        }

        private static bool IsDeleted(IList<byte> hdr)
        {
            return (hdr[(int)HdrPos.Flags] & 1) > 0;
        }
    }
}
