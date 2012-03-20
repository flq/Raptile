using System;
using System.Collections.Generic;
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
            byte[] bkey = key.ToByteArray();
            var hc = (int)Converter.MurMur.Hash(bkey);

            if (_db.Get(hc, out val))
            {
                // unpack data
                byte[] g;
                if (val.UnpackData(out val, out g))
                {
                    if (Converter.CompareMemCmp(bkey, g) != 0)
                    {
                        // if data not equal check duplicates (hash conflict)
                        var ints = new List<int>(_db.GetDuplicates(hc));
                        ints.Reverse();
                        foreach (int i in ints)
                        {
                            byte[] bb = _db.FetchRecordBytes(i);
                            if (bb.UnpackData(out val, out g))
                            {
                                if (Converter.CompareMemCmp(bkey, g) == 0)
                                    return true;
                            }
                        }
                        return false;
                    }
                    return true;
                }
            }
            return false;
        }

        public long Count(bool includeDuplicates)
        {
            return _db.Count(includeDuplicates);
        }

        public void Shutdown()
        {
            _db.Shutdown();
        }

        public void Dispose()
        {
            _db.Shutdown();
        }
    }
}