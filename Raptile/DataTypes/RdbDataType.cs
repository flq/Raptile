using System;

namespace Raptile.DataTypes
{
    internal class RdbDataType<T>
    {
        public static IGetBytes<T> ByteHandler()
        {
            Type type = typeof(T);

            if (type == typeof(int))
                return (IGetBytes<T>)new IntHandler();

            if (type == typeof(uint))
                return (IGetBytes<T>)new UintHandler();

            if (type == typeof(long))
                return (IGetBytes<T>)new LongHandler();

            if (type == typeof(Guid))
                return (IGetBytes<T>)new GuidHandler();

            if (type == typeof(string))
                return (IGetBytes<T>)new StringHandler();

            return null;
        }

        public static byte GetByteSize(byte keysize)
        {
            byte size = 4;
            Type t = typeof(T);

            if (t == typeof(int))
                size = 4;
            if (t == typeof(uint))
                size = 4;
            if (t == typeof(long))
                size = 8;
            if (t == typeof(Guid))
                size = 16;
            if (t == typeof(string))
                size = keysize;

            return size;
        }

        internal static object GetEmpty()
        {
            Type t = typeof(T);

            if (t == typeof(string))
                return "";

            return default(T);
        }
    }
}