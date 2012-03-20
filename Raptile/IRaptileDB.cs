using System;

namespace Raptile
{
    public interface IRaptileDB<T> : IDisposable where T : IComparable<T>
    {
        void Set(T key, byte[] val);
        bool Get(T key, out byte[] val);
    }
}