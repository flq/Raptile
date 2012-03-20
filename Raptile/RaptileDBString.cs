using System.IO;
using System.Text;
using OpenFileSystem.IO;
using Path = OpenFileSystem.IO.Path;

namespace Raptile
{
    internal class RaptileDBString : IRaptileDB<string>
    {
        readonly bool _caseSensitive;
        private readonly KeyStore<int> _db;

        public RaptileDBString(IFileSystem fileSystem, string filename, bool caseSensitive)
        {
            _db = new KeyStore<int>(fileSystem, new Path(filename));
            _caseSensitive = caseSensitive;
        }


        public void Set(string key, string val)
        {
            Set(key, Encoding.Unicode.GetBytes(val));
        }

        public void Set(string key, byte[] val)
        {
            string str = (_caseSensitive ? key : key.ToLower());
            byte[] bkey = Encoding.Unicode.GetBytes(str);
            var hc = (int)Converter.MurMur.Hash(bkey);
            var ms = new MemoryStream();
            ms.Write(Converter.GetBytes(bkey.Length, false), 0, 4);
            ms.Write(bkey, 0, bkey.Length);
            ms.Write(val, 0, val.Length);

            _db.Set(hc, ms.ToArray());
        }

        public bool Get(string key, out string val)
        {
            val = null;
            byte[] bval;
            var b = Get(key, out bval);
            if (b)
            {
                val = Encoding.Unicode.GetString(bval);
            }
            return b;
        }

        public bool Get(string key, out byte[] val)
        {
            string str = (_caseSensitive ? key : key.ToLower());
            byte[] bkey = Encoding.Unicode.GetBytes(str);
            int hc = (int)Converter.MurMur.Hash(bkey);

            if (_db.Get(hc, out val))
            {
                // unpack data
                byte[] g;
                return val.UnpackData(out val, out g);
            }
            return false;
        }

        public long Count
        {
            get { return _db.Count; }
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