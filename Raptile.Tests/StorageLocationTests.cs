using System.Diagnostics;
using NUnit.Framework;
using OpenFileSystem.IO;
using OpenFileSystem.IO.FileSystems.InMemory;
using FluentAssertions;
using System.Linq;
using Raptile;

namespace Raptile.Tests
{
    [TestFixture]
    public class StorageLocationTests
    {
        private InMemoryFileSystem _fs;
        private IDirectory _dir;

        [TestFixtureSetUp]
        public void Given()
        {
            _fs = new InMemoryFileSystem();
            var ks = new KeyStore<string>(_fs, new Path(@"c:\db\raptile.db"));
            ks.Shutdown();
            _dir = _fs.GetDirectory(@"c:\db");
        }

        [Test]
        public void correct_file_count()
        {
            var files = _dir.Files().ToList();
            files.ForEach(f => Debug.WriteLine(f.Name));
            files.Should().HaveCount(3);
        }

        [Test]
        [TestCase("raptile.mgidx")]
        [TestCase("raptile.mgdat")]
        [TestCase("raptile.mgrec")]
        public void all_files_defined(string name)
        {
            _dir.Files().Should().Contain(f => f.Name == name);
        }
    }
}