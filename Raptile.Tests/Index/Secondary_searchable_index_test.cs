using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using NUnit.Framework;
using OpenFileSystem.IO;
using OpenFileSystem.IO.FileSystems.InMemory;
using Raptile.Indices;
using FluentAssertions;
using System.Linq;
using Raptile;

namespace Raptile.Tests.Index
{
    [TestFixture]
    public class Secondary_searchable_index_test
    {
        readonly IFileSystem _fs = new InMemoryFileSystem();
        readonly Path _path = new Path(@"C:\secidx");
        private PropertyIndex<Foo> _index;

        [TestFixtureSetUp]
        public void Given()
        {
            _index = new PropertyIndex<Foo>(_fs, _path, "AwesomeIndex", f => new { f.Name });
            _index.Inspect(5, new Foo { Name = "Arthur" });
        }

        [Test]
        public void retrieves_record_number_of_known_object()
        {
            var recordNo = _index.Find<Foo>(() => new { Name = "Arthur" });
            Assert.AreNotEqual(null,recordNo);
            recordNo.Should().Be(5);
        }

        [Test]
        public void returns_null_if_not_found()
        {
            var recordNo = _index.Find<Foo>(() => new { Name = "Funchal" });
            Assert.IsNull(recordNo);
        }

        [Test]
        public void returns_null_if_wrong_type()
        {
            var recordNo = _index.Find<Bar>(() => new { Name = "Arthur" });
            Assert.IsNull(recordNo);
        }
    }
}