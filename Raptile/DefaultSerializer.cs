using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

namespace Raptile
{
    internal class DefaultSerializer : ISerializer
    {
        readonly IFormatter f = new BinaryFormatter();

        public byte[] Serialize(object obj)
        {
            var ms = new MemoryStream();
            f.Serialize(ms, obj);
            ms.Seek(0, SeekOrigin.Begin);
            var bytes = new byte[ms.Length];
            ms.Read(bytes, 0, bytes.Length);
            return bytes;
        }

        public T Deserialize<T>(byte[] contents)
        {
            var ms = new MemoryStream(contents, false);
            return (T)f.Deserialize(ms);
        }
    }
}