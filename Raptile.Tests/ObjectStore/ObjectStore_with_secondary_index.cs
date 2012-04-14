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
            _store = NewObjectStore(modifySettings: s => s.AddNamedGroup<Foo>("FoosWithB", f => f.Name.StartsWith("B")));

            //var idx = _store.Enumerate<Foo>("x").Select(f => new { f.Name, f.Period });

            //_store.AddIndex(idx);

            //
            //_store.Get<Foo>(f => f.Name == "Bla" && f.Period = new Period());

            //var foos = _store.GetFoos() <- Extension method
            //var foos = _store.GetFoos("Gustav") <- Extension method

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