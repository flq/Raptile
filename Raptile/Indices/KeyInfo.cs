namespace Raptile.Indices
{
    internal struct KeyInfo
    {
        public KeyInfo(int recnum)
        {
            RecordNumber = recnum;
            DuplicateBitmapNumber = -1;
        }
        public KeyInfo(int recnum, int bitmaprec)
        {
            RecordNumber = recnum;
            DuplicateBitmapNumber = bitmaprec;
        }
        public int RecordNumber;
        public int DuplicateBitmapNumber;
    }
}