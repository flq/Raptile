namespace Raptile.Indices
{
    internal class Page<T> 
    {
        private readonly SafeDictionary<T, KeyInfo> _tree;

        public Page()
        {
            DiskPageNumber = -1;
            RightPageNumber = -1;
            _tree = new SafeDictionary<T, KeyInfo>(Defaults.PageItemCount);
            isDirty = false;
            FirstKey = default(T);
        }
        public int DiskPageNumber;
        public int RightPageNumber;
        public T FirstKey;
        public bool isDirty;

        public SafeDictionary<T, KeyInfo> Tree { get { return _tree; } }
    }
}