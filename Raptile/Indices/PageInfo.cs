namespace Raptile.Indices
{
    internal struct PageInfo
    {
        public PageInfo(int pagenum, int uniquecount)
        {
            PageNumber = pagenum;
            UniqueCount = uniquecount;
        }
        public int PageNumber;
        public int UniqueCount;
    }
}