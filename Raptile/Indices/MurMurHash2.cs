using System;

namespace Raptile.Indices
{
    /// <summary>
    /// http://en.wikipedia.org/wiki/MurmurHash
    /// </summary>
    internal class MurmurHash2Unsafe
    {
        const UInt32 M = 0x5bd1e995;

        public UInt32 Hash(Byte[] data)
        {
            return Hash(data, 0xc58f1a7b);
        }

        const Int32 R = 24;

        public unsafe UInt32 Hash(Byte[] data, UInt32 seed)
        {
            Int32 length = data.Length;
            if (length == 0)
                return 0;
            UInt32 h = seed ^ (UInt32)length;
            Int32 remainingBytes = length & 3; // mod 4
            Int32 numberOfLoops = length >> 2; // div 4
            fixed (byte* firstByte = &(data[0]))
            {
                UInt32* realData = (UInt32*)firstByte;
                while (numberOfLoops != 0)
                {
                    UInt32 k = *realData;
                    k *= M;
                    k ^= k >> R;
                    k *= M;

                    h *= M;
                    h ^= k;
                    numberOfLoops--;
                    realData++;
                }
                switch (remainingBytes)
                {
                    case 3:
                        h ^= (UInt16)(*realData);
                        h ^= ((UInt32)(*(((Byte*)(realData)) + 2))) << 16;
                        h *= M;
                        break;
                    case 2:
                        h ^= (UInt16)(*realData);
                        h *= M;
                        break;
                    case 1:
                        h ^= *((Byte*)realData);
                        h *= M;
                        break;
                    default:
                        break;
                }
            }

            // Do a few final mixes of the hash to ensure the last few
            // bytes are well-incorporated.

            h ^= h >> 13;
            h *= M;
            h ^= h >> 15;

            return h;
        }
    }
}
