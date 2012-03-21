using NUnit.Framework;
using FluentAssertions;

namespace Raptile.Tests.Index
{
    [TestFixture]
    public class Typed_secondary_index_test : SecondaryIndexContext
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
}