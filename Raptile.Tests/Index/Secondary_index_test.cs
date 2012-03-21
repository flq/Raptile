using System;
using System.Linq;
using NUnit.Framework;
using FluentAssertions;

namespace Raptile.Tests.Index
{
    [TestFixture]
    public class Secondary_index_test : SecondaryIndexContext
    {
        private void CreateIndex()
        {
            LoadIndex("idxname", o => o is string);
        }

        [SetUp]
        public void Given()
        {
            CreateIndex();
            ToIndex(1, new Uri("http://ho"));
            ToIndex(2, "ha");
        }

        [Test]
        public void file_is_stored_in_correct_directory()
        {
            Index.Dispose();
            var d = Fs.GetDirectory(@"c:\db");
            d.Files().Should().HaveCount(1);
            d.Files().First().Extension.Should().Be(DbFiles.SecondaryIndexExtension);
        }

        [Test]
        public void index_returns_matching_record_number()
        {
            IndexContents.Should().HaveCount(1);
            IndexContents.First().Should().Be(2);
        }

        [Test]
        public void contents_are_stored_to_disk()
        {
            ReloadIndex();
            index_returns_matching_record_number();
        }

        [Test]
        public void record_is_removable()
        {
            Remove(2);
            IndexContents.Should().HaveCount(0);
        }
    }
}