﻿using System;
using System.IO;
using OpenFileSystem.IO;
using Path = OpenFileSystem.IO.Path;

namespace Raptile
{
    internal class RaptileDBGuid : IRaptileDB<Guid>
    {
        private readonly KeyStore<int> _db;

        public RaptileDBGuid(IFileSystem fileSystem, string filename)
        {
            _db = new KeyStore<int>(fileSystem, new Path(filename));
        }

        public void Set(Guid key, byte[] val)
        {
            var bkey = key.ToByteArray();
            var hc = (int)Converter.MurMur.Hash(bkey);
            var ms = new MemoryStream();
            ms.Write(Converter.GetBytes(bkey.Length, false), 0, 4);
            ms.Write(bkey, 0, bkey.Length);
            ms.Write(val, 0, val.Length);

            _db.Set(hc, ms.ToArray());
        }

        public bool Get(Guid key, out byte[] val)
        {
            var hc = GetHash(key);

            if (_db.Get(hc, out val))
            {
                // unpack data
                byte[] g;
                return val.UnpackData(out val, out g);
            }
            return false;
        }

        public bool Remove(Guid key)
        {
            return _db.Remove(GetHash(key));
        }

        public long Count
        {
            get { return _db.Count; }
        }

        public void Dispose()
        {
            _db.Dispose();
        }

        private static int GetHash(Guid key)
        {
            byte[] bkey = key.ToByteArray();
            return (int)Converter.MurMur.Hash(bkey);
        }
    }
}