namespace Raptile.DataTypes
{
    internal interface IGetBytes<T>
    {
        byte[] GetBytes(T obj);
        T GetObject(byte[] buffer, int offset, int count);
    }
}