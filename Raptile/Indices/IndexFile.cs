using System;
using System.Collections.Generic;
using System.IO;
using OpenFileSystem.IO;
using Raptile.DataTypes;
using Path = OpenFileSystem.IO.Path;

namespace Raptile.Indices
{
    internal class IndexFile<K> : IDisposable
    {
        private byte[] _fileHeader = new byte[] {
            (byte)'M', (byte)'G', (byte)'I',
            0,               // 3 = [keysize]   max 255
            0,0,             // 4 = [node size] max 65536
            0,0,0,0,         // 6 = [root page num]
            0,               // 10 = Index file type : 0=mgindex   --0=BTREE 1=HASH 2= 
            0,0,0,0          // 11 = last record number indexed 
            };

        private readonly byte[] _blockHeader = new byte[] { 
            (byte)'P',(byte)'A',(byte)'G',(byte)'E',
            0,               // 4 = [Flag] = 0=page 1=page list    --0=free 1=leaf 2=root 4=revisionpage --8=bucket 16=revisionbucket
            0,0,             // 5 = [item count] 
            0,0,0,0,         // 7 = reserved               --[parent page number] / [bucket number]
            0,0,0,0          // 11 = [right page number]   -- /[next page number]
        };

        private readonly ILog _log = LogManager.GetLogger(typeof(IndexFile<K>));
        readonly Stream _fileStream;
        private byte _maxKeySize;
        private ushort _pageNodeCount = 5000;
        private int _lastPageNumber = 1; // 0 = page list
        private readonly int _pageLength;
        private readonly int _rowSize;
        readonly IGetBytes<K> _byteReader;

        public IndexFile(IFileSystem fs, Path file, byte maxKeySize, ushort pageNodeCount)
        {
            _byteReader = RdbDataType<K>.ByteHandler();
            _maxKeySize = maxKeySize;
            _pageNodeCount = pageNodeCount;
            _rowSize = (_maxKeySize + 1 + 4 + 4);

            var fl = fs.GetFile(file);
            var fileExists = fl.Exists;

            _fileStream = fl.Open(FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
            
            if (fileExists)
            {
                ReadFileHeader();
                // compute last page number from file length
                _pageLength = (_blockHeader.Length + _rowSize * (_pageNodeCount));
            }
            else
            {
                _pageLength = (_blockHeader.Length + _rowSize * (_pageNodeCount));
                CreateFileHeader(0);
            }
            CalculateLastPageNumber();
        }

        private void CalculateLastPageNumber()
        {
            _lastPageNumber = (int)((_fileStream.Length - _fileHeader.Length) / _pageLength);
            if (_lastPageNumber == 0)
                _lastPageNumber = 1;
        }

        private byte[] CreateBlockHeader(byte type, ushort itemcount, int rightpagenumber)
        {
            var block = new byte[_blockHeader.Length];
            Array.Copy(_blockHeader, block, block.Length);
            block[4] = type;
            byte[] b = Converter.GetBytes(itemcount, false);
            Buffer.BlockCopy(b, 0, block, 5, 2);
            b = Converter.GetBytes(rightpagenumber, false);
            Buffer.BlockCopy(b, 0, block, 11, 4);
            return block;
        }

        private void CreateFileHeader(int rowsindexed)
        {
            // max key size
            byte[] b = Converter.GetBytes(_maxKeySize, false);
            Buffer.BlockCopy(b, 0, _fileHeader, 3, 1);
            // page node count
            b = Converter.GetBytes(_pageNodeCount, false);
            Buffer.BlockCopy(b, 0, _fileHeader, 4, 2);
            b = Converter.GetBytes(rowsindexed, false);
            Buffer.BlockCopy(b, 0, _fileHeader, 11, 4);

            _fileStream.Seek(0L, SeekOrigin.Begin);
            _fileStream.Write(_fileHeader, 0, _fileHeader.Length);
            if (rowsindexed == 0)
            {
                var pagezero = new byte[_pageLength];
                byte[] block = CreateBlockHeader(1, 0, -1);
                Buffer.BlockCopy(block, 0, pagezero, 0, block.Length);
                _fileStream.Write(pagezero, 0, _pageLength);
            }
            _fileStream.Flush();
        }

        private void ReadFileHeader()
        {
            _fileStream.Seek(0L, SeekOrigin.Begin);
            var b = new byte[_fileHeader.Length];
            _fileStream.Read(b, 0, _fileHeader.Length);

            if (b[0] == _fileHeader[0] && b[1] == _fileHeader[1] && b[2] == _fileHeader[2])
            {
                byte maxks = b[3];
                var nodes = (ushort)Converter.ToInt16(b, 4);
                _maxKeySize = maxks;
                _pageNodeCount = nodes;
                _fileHeader = b;
            }
        }

        public int GetNewPageNumber()
        {
            return _lastPageNumber++;
        }

        public void Dispose()
        {
            _log.Info("Shutdown of IndexFile");
            if (_fileStream != null)
                _fileStream.Dispose();
        }

        public void GetPageList(List<int> pageListDiskPages, SortedList<K, PageInfo> pageList, out int lastIndexedRow)
        {
            lastIndexedRow = Converter.ToInt32(_fileHeader, 11);
            // load page list
            pageListDiskPages.Add(0); // first page list
            int nextpage = LoadPageListData(0, pageList);
            while (nextpage != -1)
            {
                nextpage = LoadPageListData(nextpage, pageList);
                if (nextpage != -1)
                    pageListDiskPages.Add(nextpage);
            }
        }

        private void SeekPage(int pnum)
        {
            long offset = _fileHeader.Length;
            offset += (long)pnum * _pageLength;
            if (offset > _fileStream.Length)
                CreateBlankPages(pnum);

            _fileStream.Seek(offset, SeekOrigin.Begin);
        }

        private void CreateBlankPages(int pnum)
        {
            // create space
            var b = new byte[_pageLength];
            _fileStream.Seek(0L, SeekOrigin.Current);
            for (int i = pnum; i < _lastPageNumber; i++)
                _fileStream.Write(b, 0, b.Length);

            _fileStream.Flush();
        }

        private int LoadPageListData(int page, IDictionary<K, PageInfo> pageList)
        {
            // load page list data
            int nextpage;
            SeekPage(page);
            var b = new byte[_pageLength];
            _fileStream.Read(b, 0, _pageLength);

            if (b[0] == _blockHeader[0] && b[1] == _blockHeader[1] && b[2] == _blockHeader[2] && b[3] == _blockHeader[3])
            {
                short count = Converter.ToInt16(b, 5);
                if (count > _pageNodeCount)
                    throw new Exception("Count > node size");
                nextpage = Converter.ToInt32(b, 11);
                int index = _blockHeader.Length;

                for (int i = 0; i < count; i++)
                {
                    int idx = index + _rowSize * i;
                    byte ks = b[idx];
                    K key = _byteReader.GetObject(b, idx + 1, ks);
                    int pagenum = Converter.ToInt32(b, idx + 1 + _maxKeySize);
                    // add counts
                    int unique = Converter.ToInt32(b, idx + 1 + _maxKeySize + 4);
                    // FEATURE : add dup count
                    pageList.Add(key, new PageInfo(pagenum, unique));
                }
            }
            else
                throw new Exception("Page List header is invalid");

            return nextpage;
        }

        internal void SavePage(Page<K> node)
        {
            int pnum = node.DiskPageNumber;
            if (pnum > _lastPageNumber)
                throw new Exception("should not be here: page out of bounds");

            SeekPage(pnum);
            var page = new byte[_pageLength];
            var blockheader = CreateBlockHeader(0, (ushort)node.Tree.Count, node.RightPageNumber);
            Buffer.BlockCopy(blockheader, 0, page, 0, blockheader.Length);

            var index = blockheader.Length;
            var i = 0;
            byte[] b;
            K[] keys = node.Tree.Keys();
            // node children
            foreach (var kp in keys)
            {
                var val = node.Tree[kp];
                int idx = index + _rowSize * i++;
                // key bytes
                byte[] kk = _byteReader.GetBytes(kp);
                byte size = (byte)kk.Length;
                if (size > _maxKeySize)
                    size = _maxKeySize;
                // key size = 1 byte
                page[idx] = size;
                Buffer.BlockCopy(kk, 0, page, idx + 1, page[idx]);
                // offset = 4 bytes
                b = Converter.GetBytes(val.RecordNumber, false);
                Buffer.BlockCopy(b, 0, page, idx + 1 + _maxKeySize, b.Length);
                // duplicatepage = 4 bytes
                b = Converter.GetBytes(val.DuplicateBitmapNumber, false);
                Buffer.BlockCopy(b, 0, page, idx + 1 + _maxKeySize + 4, b.Length);
            }
            _fileStream.Write(page, 0, page.Length);
        }

        public Page<K> LoadPageFromPageNumber(int number)
        {
            SeekPage(number);
            var b = new byte[_pageLength];
            _fileStream.Read(b, 0, _pageLength);

            if (b[0] == _blockHeader[0] && b[1] == _blockHeader[1] && b[2] == _blockHeader[2] && b[3] == _blockHeader[3])
            {
                // create node here
                var page = new Page<K>();

                short count = Converter.ToInt16(b, 5);
                if (count > _pageNodeCount)
                    throw new Exception("Count > node size");
                page.DiskPageNumber = number;
                page.RightPageNumber = Converter.ToInt32(b, 11);
                int index = _blockHeader.Length;

                for (int i = 0; i < count; i++)
                {
                    int idx = index + _rowSize * i;
                    byte ks = b[idx];
                    K key = _byteReader.GetObject(b, idx + 1, ks);
                    int offset = Converter.ToInt32(b, idx + 1 + _maxKeySize);
                    int duppage = Converter.ToInt32(b, idx + 1 + _maxKeySize + 4);
                    page.Tree.Add(key, new KeyInfo(offset, duppage));
                }
                return page;
            }
            throw new InvalidOperationException("Page read error header invalid, number = " + number);
        }


        internal void SavePageList(SortedList<K, PageInfo> pages, List<int> diskpages)
        {
            // save page list
            int c = (pages.Count / Defaults.PageItemCount) + 1;
            // allocate pages needed 
            while (c > diskpages.Count)
                diskpages.Add(GetNewPageNumber());

            for (int i = 0; i < (diskpages.Count - 1); i++)
            {
                var page = new byte[_pageLength];
                byte[] block = CreateBlockHeader(1, Defaults.PageItemCount, diskpages[i + 1]);
                Buffer.BlockCopy(block, 0, page, 0, block.Length);
                for (int j = 0; j < Defaults.PageItemCount; j++)
                {
                    CreatePageListData(pages, i, page, block.Length, j);
                }
                SeekPage(diskpages[i]);
                _fileStream.Write(page, 0, page.Length);
            }
            c = pages.Count % Defaults.PageItemCount;
            byte[] lastblock = CreateBlockHeader(1, (ushort)c, -1);
            var lastpage = new byte[_pageLength];
            Buffer.BlockCopy(lastblock, 0, lastpage, 0, lastblock.Length);
            for (int j = 0; j < c; j++)
            {
                CreatePageListData(pages, diskpages.Count - 1, lastpage, lastblock.Length, j);
            }
            SeekPage(diskpages[diskpages.Count - 1]);
            _fileStream.Write(lastpage, 0, lastpage.Length);
        }

        private void CreatePageListData(SortedList<K, PageInfo> pages, int i, byte[] page, int index, int j)
        {
            int idx = index + _rowSize * j;
            // key bytes
            byte[] kk = _byteReader.GetBytes(pages.Keys[j + i]);
            var size = (byte)kk.Length;
            if (size > _maxKeySize)
                size = _maxKeySize;
            // key size = 1 byte
            page[idx] = size;
            Buffer.BlockCopy(kk, 0, page, idx + 1, page[idx]);
            // offset = 4 bytes
            var b = Converter.GetBytes(pages.Values[i + j].PageNumber, false);
            Buffer.BlockCopy(b, 0, page, idx + 1 + _maxKeySize, b.Length);
            // add counts 
            b = Converter.GetBytes(pages.Values[i + j].UniqueCount, false);
            Buffer.BlockCopy(b, 0, page, idx + 1 + _maxKeySize + 4, b.Length);
        }

        internal void SaveLastRecordNumber(int recnum)
        {
            // save the last record number indexed to the header
            CreateFileHeader(recnum);
        }
    }
}