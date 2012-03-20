using System;
using System.Collections.Generic;
using System.Linq;
using OpenFileSystem.IO;
using Raptile.DataTypes;

namespace Raptile.Indices
{
    /// <summary>
    /// http://www.codeproject.com/Articles/316816/RaptorDB-The-Key-Value-Store-V2
    /// </summary>
    internal class MGIndex<T> : IDisposable where T : IComparable<T>
    {
        private readonly ILog _log = LogManager.GetLogger(typeof(MGIndex<T>));
        private readonly object _savelock = new object();
        private readonly SortedList<T, PageInfo> _pageList = new SortedList<T, PageInfo>();
        private readonly SafeDictionary<int, Page<T>> _cache = new SafeDictionary<int, Page<T>>();
        private readonly List<int> _pageListDiskPages = new List<int>();
        private readonly IndexFile<T> _index;
        
        private double _totalsplits;
        private int _lastIndexedRecordNumber;

        public MGIndex(IFileSystem fs, Path path, byte keysize, ushort maxcount)
        {
            _index = new IndexFile<T>(fs, path, keysize, maxcount);
            _index.GetPageList(_pageListDiskPages, _pageList, out _lastIndexedRecordNumber);
            
            if (_pageList.Count != 0) return;
            
            CreateFirstPage();
        }

        public int Count
        {
            get
            {
                return _pageList.Sum(k => k.Value.UniqueCount);
            }
        }

        public int GetLastIndexedRecordNumber()
        {
            return _lastIndexedRecordNumber;
        }

        public void Set(T key, int recordNumber)
        {
            PageInfo pi;
            Page<T> page = LoadPage(key, out pi);

            KeyInfo ki;
            if (page.Tree.TryGetValue(key, out ki))
            {
                ki.RecordNumber = recordNumber;
                page.Tree[key] = ki; // structs need resetting
            }
            else
            {
                // new item 
                ki = new KeyInfo(recordNumber);
                pi.UniqueCount++;
                page.Tree.Add(key, ki);
            }

            if (page.Tree.Count > Global.PageItemCount)
                SplitPage(page);

            _lastIndexedRecordNumber = recordNumber;
            page.isDirty = true;
        }

        public bool Get(T key, out int val)
        {
            val = -1;
            PageInfo pi;
            Page<T> page = LoadPage(key, out pi);
            KeyInfo ki;
            bool ret = page.Tree.TryGetValue(key, out ki);
            if (ret)
                val = ki.RecordNumber;
            return ret;
        }

        public void SaveIndex()
        {
            lock (_savelock)
            {
                _log.Debug("persisting index");
                _log.Debug("Total pages = " + _pageList.Count);
                var keys = new List<int>(_cache.Keys());
                keys.Sort();
                foreach (var p in keys.Select(i => _cache[i]).Where(p => p.isDirty))
                {
                    _index.SavePage(p);
                    p.isDirty = false;
                }
            }
            _log.Debug("index persisted");
        }

        public void Dispose()
        {
            SaveIndex();
            _index.SavePageList(_pageList, _pageListDiskPages);
            _index.Dispose();
        }

        public IEnumerable<KeyValuePair<T, int>> Enumerate(T fromkey)
        {
            var list = new List<KeyValuePair<T, int>>();
            // enumerate
            PageInfo pi;
            Page<T> page = LoadPage(fromkey, out pi);
            T[] keys = page.Tree.Keys();
            Array.Sort(keys);

            int p = Array.BinarySearch(keys, fromkey);
            for (int i = p; i < keys.Length; i++)
                list.Add(new KeyValuePair<T, int>(keys[i], page.Tree[keys[i]].RecordNumber));

            while (page.RightPageNumber != -1)
            {
                page = LoadPage(page.RightPageNumber);
                keys = page.Tree.Keys();
                Array.Sort(keys);

                list.AddRange(keys.Select(k => new KeyValuePair<T, int>(k, page.Tree[k].RecordNumber)));
            }

            return list;
        }

        public void SaveLastRecordNumber(int recnum)
        {
            _index.SaveLastRecordNumber(recnum);
        }

        public bool RemoveKey(T key)
        {
            PageInfo pi;
            Page<T> page = LoadPage(key, out pi);
            bool b = page.Tree.Remove(key);
            if (b)
                page.isDirty = true;
            return b;
        }

        public Statistics GetStatistics()
        {
            var s = new Statistics {TotalSplitTime = _totalsplits, PageCount = _pageList.Count};
            return s;
        }

        private void SplitPage(Page<T> page)
        {
            // split the page
            DateTime dt = FastDateTime.Now;

            var newpage = new Page<T>
                              {
                                  DiskPageNumber = _index.GetNewPageNumber(),
                                  RightPageNumber = page.RightPageNumber,
                                  isDirty = true
                              };
            page.RightPageNumber = newpage.DiskPageNumber;
            // get and sort keys
            T[] keys = page.Tree.Keys();
            Array.Sort(keys);
            // copy data to new 
            for (int i = keys.Length / 2; i < keys.Length; i++)
            {
                newpage.Tree.Add(keys[i], page.Tree[keys[i]]);
                // remove from old page
                page.Tree.Remove(keys[i]);
            }
            // set the first key
            newpage.FirstKey = keys[keys.Length / 2];
            // set the first key refs
            _pageList.Remove(page.FirstKey);
            _pageList.Remove(keys[0]);
            // dup counts
            _pageList.Add(keys[0], new PageInfo(page.DiskPageNumber, page.Tree.Count));
            page.FirstKey = keys[0];
            // FEATURE : dup counts
            _pageList.Add(newpage.FirstKey, new PageInfo(newpage.DiskPageNumber, newpage.Tree.Count));
            _cache.Add(newpage.DiskPageNumber, newpage);

            _totalsplits += FastDateTime.Now.Subtract(dt).TotalSeconds;
        }

        private Page<T> LoadPage(T key, out PageInfo pageinfo)
        {
            // find page in list of pages

            bool found = false;
            int pos = 0;
            if (key != null)
                pos = FindPageOrLowerPosition(key, ref found);
            pageinfo = _pageList.Values[pos];
            int pagenum = pageinfo.PageNumber;

            Page<T> page;
            if (_cache.TryGetValue(pagenum, out page) == false)
            {
                //load page from disk
                page = _index.LoadPageFromPageNumber(pagenum);
                _cache.Add(pagenum, page);
            }
            return page;
        }

        private Page<T> LoadPage(int pagenum)
        {
            Page<T> page;
            if (_cache.TryGetValue(pagenum, out page) == false)
            {
                //load page from disk
                page = _index.LoadPageFromPageNumber(pagenum);
                _cache.Add(pagenum, page);
            }
            return page;
        }

        private void CreateFirstPage()
        {
            var page = new Page<T>
                           {
                               FirstKey = (T) RdbDataType<T>.GetEmpty(),
                               DiskPageNumber = _index.GetNewPageNumber(),
                               isDirty = true
                           };
            _pageList.Add(page.FirstKey, new PageInfo(page.DiskPageNumber, 0));
            _cache.Add(page.DiskPageNumber, page);
        }

        private int FindPageOrLowerPosition(T key, ref bool found)
        {
            if (_pageList.Count == 0)
                return 0;
            // binary search
            var lastlower = 0;
            int first = 0;
            int last = _pageList.Count - 1;
            while (first <= last)
            {
                int mid = (first + last) >> 1;
                T k = _pageList.Keys[mid];
                int compare = k.CompareTo(key);
                if (compare < 0)
                {
                    lastlower = mid;
                    first = mid + 1;
                }
                if (compare == 0)
                {
                    found = true;
                    return mid;
                }
                if (compare > 0)
                {
                    last = mid - 1;
                }
            }

            return lastlower;
        }
    }

    public class Statistics
    {
        public int PageCount;
        public double TotalSplitTime;
        public double FillFactor;

        public override string ToString()
        {
            return "Page Count = " + PageCount + ", Total Split Time = " + TotalSplitTime;
        }
    }
}
