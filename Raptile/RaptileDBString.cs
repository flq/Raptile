using System.IO;
using System.Text;
using OpenFileSystem.IO;

namespace Raptile
{
    internal class RaptileDBString : IRaptileDB<string>, IRaptileInternalDB
    {
        private readonly KeyStore<int> _db;

        public RaptileDBString(IFileSystem fileSystem, Settings settings)
        {
            _db = new KeyStore<int>(fileSystem, settings);
        }

        public void Set(string key, string val)
        {
            Set(key, Encoding.Unicode.GetBytes(val));
        }

        public int Set(string key, byte[] val)
        {
            byte[] bkey = Encoding.Unicode.GetBytes(key);
            var hc = (int)Converter.MurMur.Hash(bkey);
            var ms = new MemoryStream();
            ms.Write(Converter.GetBytes(bkey.Length, false), 0, 4);
            ms.Write(bkey, 0, bkey.Length);
            ms.Write(val, 0, val.Length);

            return _db.Set(hc, ms.ToArray());
        }

        public bool Get(string key, out byte[] val)
        {
            int hc = GetKeyHash(key);

            if (_db.Get(hc, out val))
            {
                // unpack data
                byte[] g;
                return val.UnpackData(out val, out g);
            }
            return false;
        }

        public bool Remove(string key)
        {
            return _db.Remove(GetKeyHash(key));
        }

        public long Count
        {
            get { return _db.Count; }
        }

        public void Dispose()
        {
            _db.Dispose();
        }

        private static int GetKeyHash(string key)
        {
            byte[] bkey = Encoding.Unicode.GetBytes(key);
            return (int)Converter.MurMur.Hash(bkey);
        }

        byte[] IRaptileInternalDB.ReadData(int recordNumber)
        {
            byte[] g;
            var bytes = ((IRaptileInternalDB)_db).ReadData(recordNumber);
            bytes.UnpackData(out bytes, out g);
            return bytes;
        }
    }
}