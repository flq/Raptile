using System;
using NUnit.Framework;
using FluentAssertions;
using Raptile.Indices;
using System.Linq;

namespace Raptile.Tests.Index
{
    [TestFixture]
    public class Typed_named_group_test : SecondaryIndexContext
    {
        [TestFixtureSetUp]
        public void Given()
        {
            LoadIndex<string>("myindex", s => s.StartsWith("A"));
            ToIndex(13, "Aaron");
            ToIndex(12, "Beelzebub");
        }

        [Test]
        public void correct_index_stored()
        {
            IndexContents.Should().HaveCount(1);
            IndexContents.Should().Contain(13);
        }
    }

    [TestFixture]
    public class Secondary_index_container : SecondaryIndexContext
    {
        SecondaryIndexContainer _cnt;

        [SetUp]
        public void Given()
        {
            _cnt = new SecondaryIndexContainer();
            LoadIndex("firstIndex", s => s is Uri);
            _cnt.AddIndex(Index);
            LoadIndex<string>("secondIndex", s => s.StartsWith("A"));
            _cnt.AddIndex(Index);

            _cnt.Inspect(0, new Uri("http://uri"));
            _cnt.Inspect(1, "Beelzebub");
            _cnt.Inspect(2, "Aaron");
        }

        [Test]
        [TestCase("firstIndex", 1)]
        [TestCase("secondIndex", 1)]
        [TestCase("thirdIndex", 0)]
        public void enumerate_index_verify_count(string name, int count)
        {
            var l = _cnt.Enumerate(name).ToList();
            l.Should().HaveCount(count);
        }

        [Test]
        public void all_files_are_stored()
        {
            _cnt.Dispose();
            var f = Fs.GetDirectory(@"c:\db").Files();
            f.Should().HaveCount(2);
        }
    }
}