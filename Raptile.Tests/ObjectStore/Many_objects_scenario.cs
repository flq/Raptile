using System.IO;
using NUnit.Framework;
using FluentAssertions;

namespace Raptile.Tests.ObjectStore
{
    [TestFixture]
    public class Many_objects_scenario : IntegrationContext
    {
        private IObjectStore<string> _store;

        [TestFixtureSetUp]
        public void Given()
        {
            _store = NewObjectStore();

            for (var i = 0; i < 4000; i++)
                _store.Set(i.ToString(), new Foo {Name = Path.GetRandomFileName()});

            _store.Set("12345", new Foo { Name = "Alfred" });

            for (var i = 4000; i < 8000; i++)
                _store.Set(i.ToString(), new Foo {Name = Path.GetRandomFileName()});

            _store.Set("54321", new Foo { Name = "Arthur" });

            _store = ReloadObjectStore();
        }

        [Test]
        [TestCase("54321", "Arthur")]
        [TestCase("12345", "Alfred")]
        public void known_object_check(string key, string name)
        {
            var f = _store.Get<Foo>(key);
            f.Should().NotBeNull();
            f.Name.Should().Be(name);
        }

        [Test]
        [TestCase("13")]
        [TestCase("1013")]
        [TestCase("7013")]
        [TestCase("7999")]
        public void known_index_check(string key)
        {
            var f = _store.Get<Foo>(key);
            f.Should().NotBeNull();
        }
        
        [Test]
        [TestCase("foo")]
        [TestCase("8001")]
        public void unknown_index_check(string key)
        {
            var f = _store.Get<Foo>(key);
            f.Should().BeNull();
        }
    }
}