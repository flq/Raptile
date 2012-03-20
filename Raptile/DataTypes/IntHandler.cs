namespace Raptile.DataTypes
{
    internal class IntHandler : IGetBytes<int>
    {
        public byte[] GetBytes(int obj)
        {
            return Converter.GetBytes(obj, false);
        }

        public int GetObject(byte[] buffer, int offset, int count)
        {
            return Converter.ToInt32(buffer, offset);
        }
    }
}