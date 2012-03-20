namespace Raptile.DataTypes
{
    internal class UintHandler : IGetBytes<uint>
    {
        public byte[] GetBytes(uint obj)
        {
            return Converter.GetBytes(obj, false);
        }

        public uint GetObject(byte[] buffer, int offset, int count)
        {
            return (uint)Converter.ToInt32(buffer, offset);
        }
    }
}