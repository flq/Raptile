using NUnit.Framework;
using FluentAssertions;

namespace Raptile.Tests.Storage
{
    [TestFixture]
    public class Storage_file_basic_usage : StorageFileTestContext
    {
        [TestFixtureSetUp]
        public void Given()
        {
            Store("a", "world");
            Store("b", "hello");
        }

        [Test]
        public void both_keys_are_retrievable()
        {
            Get("a").Should().Be("world");
            Get("b").Should().Be("hello");
        }
    }
}