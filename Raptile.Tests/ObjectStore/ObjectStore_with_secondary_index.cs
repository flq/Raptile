using System.Linq;
using NUnit.Framework;
using FluentAssertions;
using System;

namespace Raptile.Tests.ObjectStore
{
    [TestFixture]
    public class object_store_with_secondary_index : IntegrationContext
    {
        private IObjectStore<string> _store;

        [TestFixtureSetUp]
        public void Given()
        {
            _store = NewObjectStore(modifySettings: s => { 
                s.AddNamedGroup<Foo>("FoosWithB", f => f.Name.StartsWith("B"));
                s.AddPropertyIndex<Foo>("name", f => new { f.Name });
            });

            _store.Set("123", new Foo { Name = "Belzebub"});
            _store.Set("100", new Foo { Name = "Aaron", Time = new DateTime(2011,6,6)  });
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

        [Test]
        public void retrieval_through_indexed_property()
        {
            var f1 = _store.Get<Foo>(() => new { Name = "Aaron" });
            f1.Should().NotBeNull();
            f1.Time.Should().Be(new DateTime(2011, 6, 6));
        }
    }
}