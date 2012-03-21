using System;

namespace Raptile
{
    public interface IRaptileDB<T> : IDisposable where T : IComparable<T>
    {
        int Set(T key, byte[] val);
        bool Get(T key, out byte[] val);
        bool Remove(T key);
        long Count { get; }
    }

    internal interface IRaptileInternalDB
    {
        byte[] ReadData(int recordNumber);
    }
}