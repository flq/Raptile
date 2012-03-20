using System.Diagnostics;
using System.Text;
using NUnit.Framework;
using FluentAssertions;
using Raptile.Indices;

namespace Raptile.Tests
{
    [TestFixture]
    public class MurmurHashTests
    {
        private MurmurHash2Unsafe _hasher;

        [TestFixtureSetUp]
        public void Given()
        {
            _hasher = new MurmurHash2Unsafe();
        }

        [Test]
        [TestCase("Fox", (uint)2652050779)]
        [TestCase("", (uint)0)]
        [TestCase("Hello World", (uint)3731549914)]
        [TestCase("The incredible truth about raka-huru", (uint)4023838327)]
        public void hash_comparisons(string input, uint expectedHash)
        {
            var bs = Encoding.UTF8.GetBytes(input);
            var hash = _hasher.Hash(bs);
            Debug.WriteLine(hash);
            hash.Should().Be(expectedHash);
        }
    }
}