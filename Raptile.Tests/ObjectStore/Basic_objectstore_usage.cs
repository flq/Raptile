using NUnit.Framework;
using FluentAssertions;

namespace Raptile.Tests
{
    [TestFixture]
    public class Basic_objectstore_usage : IntegrationContext
    {
        private IObjectStore<string> _store;

        [TestFixtureSetUp]
        public void Given()
        {
            _store = NewObjectStore();
            _store.Set("12345", new Foo { Name = "Alfred"});
        }

        [Test]
        public void object_is_retrievable()
        {
            var obj = _store.Get<Foo>("12345");
            obj.Should().NotBeNull();
            obj.Name.Should().Be("Alfred");
        }
    }
}