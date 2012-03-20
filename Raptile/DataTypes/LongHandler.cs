namespace Raptile.DataTypes
{
    internal class LongHandler : IGetBytes<long>
    {
        public byte[] GetBytes(long obj)
        {
            return Converter.GetBytes(obj, false);
        }

        public long GetObject(byte[] buffer, int offset, int count)
        {
            return Converter.ToInt64(buffer, offset);
        }
    }
}