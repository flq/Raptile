using NUnit.Framework;
using FluentAssertions;

namespace Raptile.Tests.Index
{
    [TestFixture]
    public class Index_file_basic_usage : IndexFileTestContext
    {

        [TestFixtureSetUp]
        public void Given()
        {
            SetKeyForRecordNumber("foo", 3);
            SetKeyForRecordNumber("bar", 345);
        }

        [Test]
        public void stored_key_point_correctly()
        {
            Get("foo").Should().Be(3);
            Get("bar").Should().Be(345);
        }
    }

    [TestFixture]
    public class Index_file_with_reload : IndexFileTestContext
    {

        [TestFixtureSetUp]
        public void Given()
        {
            SetKeyForRecordNumber("foo", 3);
            SetKeyForRecordNumber("bar", 345);
            ReloadIndex();
        }

        [Test]
        public void stored_key_point_correctly()
        {
            Get("foo").Should().Be(3);
            Get("bar").Should().Be(345);
        }
    }
}