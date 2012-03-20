namespace Raptile.DataTypes
{
    internal class StringHandler : IGetBytes<string>
    {
        public byte[] GetBytes(string obj)
        {
            return Converter.GetBytes(obj);
        }

        public string GetObject(byte[] buffer, int offset, int count)
        {
            return Converter.GetString(buffer, offset, (short)count);
        }
    }
}