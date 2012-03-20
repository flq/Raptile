using System.IO;
using ServiceStack.Text;

namespace Raptile.Tests
{
    public class ServiceStackSerializer : ISerializer
    {
        public byte[] Serialize(object obj)
        {
            var ms = new MemoryStream();
            JsonSerializer.SerializeToStream(obj, ms);
            ms.Seek(0, SeekOrigin.Begin);
            var bytes = new byte[ms.Length];
            ms.Read(bytes, 0, bytes.Length);
            return bytes;
        }

        public T Deserialize<T>(byte[] contents)
        {
            var ms = new MemoryStream(contents, false);
            return JsonSerializer.DeserializeFromStream<T>(ms);
        }
    }
}