using System.Linq;
using FluentAssertions;
using NUnit.Framework;

namespace Raptile.Tests.Storage
{
    [TestFixture]
    public class long_storage_key : StorageFileTestContext
    {
        private string _tooLongKey;

        [TestFixtureSetUp]
        public void Given()
        {
            _tooLongKey = new string(Enumerable.Repeat('a', KeyLength + 1).ToArray());
            Store(_tooLongKey, "world");
            ReloadStorage();
        }

        [Test]
        public void using_too_long_key_is_irrelevant()
        {
            Get(_tooLongKey).Should().NotBeNull();
        }
    }
}