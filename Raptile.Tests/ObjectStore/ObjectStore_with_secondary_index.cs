using System.Linq;
using NUnit.Framework;
using FluentAssertions;

namespace Raptile.Tests.ObjectStore
{
    [TestFixture]
    public class object_store_with_secondary_index : IntegrationContext
    {
        private IObjectStore<string> _store;

        [TestFixtureSetUp]
        public void Given()
        {
            _store = NewObjectStore(modifySettings: s => s.AddIndex<Foo>("FoosWithB", f => f.Name.StartsWith("B")));
            _store.Set("123", new Foo { Name = "Belzebub"});
            _store.Set("100", new Foo { Name = "Aaron" });
        }

        [Test]
        public void IndexedObjectsRetrievable()
        {
            var f1 = _store.Get<Foo>("123");
            f1.Should().NotBeNull();
            var foos = _store.Enumerate<Foo>("FoosWithB").ToList();
            foos.Should().HaveCount(1);
            foos[0].Name.Should().Be(f1.Name);
        }
    }
}