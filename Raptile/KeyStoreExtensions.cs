using System;
using System.Text;

namespace Raptile
{
    public static class KeyStoreExtensions
    {
        public static void Set<T>(this IRaptileDB<T> store, T key, string data) where T : IComparable<T>
        {
            store.Set(key, Encoding.UTF8.GetBytes(data));
        }

        internal static string FetchRecordAsString<T>(this KeyStore<T> store, int record) where T : IComparable<T>
        {
            byte[] b = store.FetchRecordBytes(record);
            return Encoding.Unicode.GetString(b);
        }

        internal static bool Get<T>(this IRaptileDB<T> store, T key, out string val) where T : IComparable<T>
        {
            byte[] b;
            val = "";
            var ret = store.Get(key, out b);
            if (ret)
                val = Encoding.Unicode.GetString(b);
            return ret;
        }

        internal static void Set<T>(this KeyStore<T> store, T key, string data) where T : IComparable<T>
        {
            store.Set(key, Encoding.Unicode.GetBytes(data));
        }

        internal static bool UnpackData(this byte[] buffer, out byte[] val, out byte[] key)
        {
            int len = Converter.ToInt32(buffer, 0, false);
            key = new byte[len];
            Buffer.BlockCopy(buffer, 4, key, 0, len);
            val = new byte[buffer.Length - 4 - len];
            Buffer.BlockCopy(buffer, 4 + len, val, 0, buffer.Length - 4 - len);
            return true;
        }
    }
}