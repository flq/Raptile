using System;

namespace Raptile.DataTypes
{
    internal class GuidHandler : IGetBytes<Guid>
    {
        public byte[] GetBytes(Guid obj)
        {
            return obj.ToByteArray();
        }

        public Guid GetObject(byte[] buffer, int offset, int count)
        {
            byte[] b = new byte[16];
            Buffer.BlockCopy(buffer, offset, b, 0, 16);
            return new Guid(b);
        }
    }
}