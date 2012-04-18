using System;
using System.Linq.Expressions;
using NUnit.Framework;
using FluentAssertions;
using Raptile.Indices;

namespace Raptile.Tests.Index
{
    [TestFixture]
    public class IndexExtractionTests
    {

        [Test]
        public void different_obj_returns_null_key  ()
        {
            var ex = GetExtractor<Foo>(f => f.Name);
            var key = ex.Read("Arnold");
            key.Should().BeNull();
        }

        [Test]
        public void single_prop_extraction()
        {
            var ex = GetExtractor<Foo>(f => f.Name);
            var key = ex.Read(new Foo { Name = "Arnold"});
            key.Should().NotBeNull();
            key.Should().Be("Arnold");
        }

        [Test]
        public void multi_prop_extraction()
        {
            var ex = GetExtractor<Foo>(f => new { f.Name, f.Time });
            var key = ex.Read(new Foo { Name = "Arnold", Time = new DateTime(2012,4,14) });
            key.Should().NotBeNull();
            key.Should().Be("{ Name = Arnold, Time = 14.04.2012 00:00:00 }");
        }

        private IndexExtractor<T> GetExtractor<T>(Expression<Func<T, object>> def)
        {
            return new IndexExtractor<T>(def);
        }
    }
}