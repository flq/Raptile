using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using NUnit.Framework;
using OpenFileSystem.IO;
using OpenFileSystem.IO.FileSystems.InMemory;
using System.Threading;
using Raptile;

namespace Raptile.Tests
{
    [TestFixture]
    public class Tests
    {
        const string String1Kb =
            "{\"$type\":\"BizFX.TestApp.Entity.Customer, BizFX.TestApp.Entity, Version=1.0.0.0, Culture=neutral, PublicKeyToken=426204062733118a\",\"Address\":null,\"Code\":null,\"Phone\":null,\"email\":null,\"Mobile\":null,\"ContactName\":null,\"Comments\":null,\"GUID\":\"iNIaPond7k6McmSStz14kA==\",\"BaseInfo\":{\"$type\":\"BizFX.Entity.BaseInfo, BizFX.Entity, Version=2.0.0.0, Culture=neutral, PublicKeyToken=e5d192f5e46064af\",\"RevisionNumber\":0,\"CreateDate\":\"2011-04-06 10:16:50\",\"SkipSync\":false,\"SkipDocs\":false,\"SkipRunning\":false,\"DeleteRevisions\":false,\"AssemblyFilename\":\"BizFX.TestApp.Entity.Customer, BizFX.TestApp.Entity, Version=1.0.0.0, Culture=neutral, PublicKeyToken=426204062733118a\",\"TypeName\":\"BizFX.TestApp.Entity.Customer\"},\"SecurityInfo\":{\"$type\":\"BizFX.Entity.SecurityInfo, BizFX.Entity, Version=2.0.0.0, Culture=neutral, PublicKeyToken=e5d192f5e46064af\",\"WinUserName\":\"\",\"AppUserName\":\"\",\"GUID\":\"FAfCsxxJOUuLJITZj005Ow==\",\"LoginName\":\"\",\"UserName\":\"\",\"MachineName\":\"\",\"UserDomainName\":\"\"},\"Description\":\"Base entity description.\",\"Name\":\"BaseEntity\"}";

        private readonly IFileSystem _fileSystem = new InMemoryFileSystem();

        [Test]
        public void Enumerate()
        {
            var db = new KeyStore<Guid>(_fileSystem, new Path("raptile.db"));
            var sk = Guid.NewGuid();

            db.Set(sk, "find key");

            for (int i = 0; i < 100000; i++)
            {
                db.Set(Guid.NewGuid(), "" + i);
            }

            int found = 0;
            foreach (var enu in db.Enumerate(sk))
            {
                if (found == 0)
                {
                    var str = db.FetchRecordAsString(enu.Value);
                    if (str != "find key")
                    {
                        Debug.WriteLine(str);
                        Assert.Fail();
                    }
                }
                found++;
            }
            Debug.WriteLine("Enumerate from key count = " + found);
            db.RemoveKey(sk);
            db.Shutdown();
        }


        [Test]
        public void ten_thousand_set_get()
        {
            set_get("ten thousand", 10000, false, false);
        }

        [Test]
        public void ten_thousand_set_shutdown_get()
        {
            set_get("ten_thousand_with_shutdown", 10000, true, false);
        }

        [Test,Ignore]
        public void Multithread_test()
        {
            //  write this test -> 2 write threads , 1 read thread after 5 sec delay
            var db = new KeyStore<Guid>(_fileSystem, new Path("multithread"));

            DateTime dt = DateTime.Now;
            threadtest(db);
            Console.WriteLine("\r\ntotal time = " + DateTime.Now.Subtract(dt).TotalSeconds);
            db.Dispose();
        }

        private static void insertthread(KeyStore<Guid> rap, List<Guid> guids, int start, int count, char c)
        {
            for (int i = 0; i < count; i++)
            {
                rap.Set(guids[i + start], "" + (i + start));

                if (i % 100000 == 0)
                {
                    Console.Write(c);
                }
            }
        }

        private static void readthread(KeyStore<Guid> rap, List<Guid> guids, int count, char c)
        {
            Thread.Sleep(5000);
            int notfound = 0;
            for (int i = 0; i < count; i++)
            {
                string bb;
                if (rap.Get(guids[i], out bb))
                {
                    if (bb != "" + i)
                        notfound++;
                }
                else
                    notfound++;
                if (i % 100000 == 0)
                {
                    Console.Write(c);
                }
            }
            if (notfound > 0)
            {
                Console.WriteLine("not found = " + notfound);
                Assert.Fail();
            }
            Console.WriteLine("read done");
        }

        private static void threadtest(KeyStore<Guid> rap)
        {
            const int count = 1000000;
            var guids = new List<Guid>();
            Console.WriteLine("building list...");
            for (int i = 0; i < 2 * count; i++)
                guids.Add(Guid.NewGuid());
            Console.WriteLine("starting...");
            var t1 = new Thread(() => insertthread(rap, guids, 0, count, '.'));
            var t2 = new Thread(() => insertthread(rap, guids, count, count, '-'));
            var t3 = new Thread(() => readthread(rap, guids, count, 'R'));
            t3.Start();
            t2.Start();
            t1.Start();
            t3.Join();
            t2.Join();
            t1.Join();
        }

        private void set_get(string fname, int count, bool shutdown, bool skiplist)
        {
            Console.WriteLine("One million test on ");
            var db = new KeyStore<Guid>(_fileSystem, new Path(fname));

            var guids = new List<Guid>();
            if (skiplist == false)
            {
                Console.Write("Building guid list...");
                for (int i = 0; i < count; i++)
                    guids.Add(Guid.NewGuid());
            }
            Console.WriteLine("done");
            DateTime dt = DateTime.Now;
            int c = 0;
            if (skiplist == false)
            {
                foreach (Guid g in guids)
                {
                    string s = "" + g;
                    db.Set(g, Encoding.Unicode.GetBytes(s));
                    c++;
                    if (c % 10000 == 0)
                        Console.Write(".");
                    if (c % 100000 == 0)
                        Console.WriteLine("time = " + DateTime.Now.Subtract(dt).TotalSeconds);
                }
            }
            else
            {
                for (int k = 0; k < count; k++)
                {
                    Guid g = Guid.NewGuid();
                    string s = "" + g;
                    db.Set(g, Encoding.Unicode.GetBytes(s));
                    c++;
                    if (c % 10000 == 0)
                        Console.Write(".");
                    if (c % 100000 == 0)
                        Console.WriteLine("time = " + DateTime.Now.Subtract(dt).TotalSeconds);
                }
            }

            Console.WriteLine(count.ToString("#,#") + " save total time = " + DateTime.Now.Subtract(dt).TotalSeconds);
            dt = DateTime.Now;
            if (shutdown)
            {
                db.Dispose();
                db = new KeyStore<Guid>(_fileSystem, new Path(fname));
            }
            GC.Collect(2);
            int notfound = 0;
            c = 0;
            if (skiplist == false)
            {
                foreach (Guid g in guids)
                {
                    byte[] val;
                    if (db.Get(g, out val))
                    {
                        string s = Encoding.Unicode.GetString(val);
                        if (s.Equals("" + g) == false)
                            Assert.Fail("data does not match " + g);
                    }
                    else
                    {
                        notfound++;
                        //Assert.Fail("item not found " + g);
                    }
                    c++;
                    if (c % 100000 == 0)
                        Console.Write(".");
                }
                if (notfound > 0)
                {
                    Console.WriteLine("items not found  = " + notfound);
                    Assert.Fail("items not found");
                }
                Console.WriteLine("\r\nfetch total time = " + DateTime.Now.Subtract(dt).TotalSeconds);
            }
            Console.WriteLine("ALL DONE OK");
            //db.Shutdown();
            db.Dispose();
        }

        [Test]
        public void RemoveKeyTest()
        {
            var path = new Path("remove.dat");
            var rdb = new KeyStore<long>(_fileSystem, path);
            rdb.Set(1, "a");
            rdb.Set(2, "b");
            rdb.Shutdown();


            rdb = new KeyStore<long>(_fileSystem, path);
            rdb.RemoveKey(1);
            rdb.Shutdown();


            rdb = new KeyStore<long>(_fileSystem, path);
            string data;
            bool result = rdb.Get(1, out data);
            if (result)
                Assert.Fail();
        }

        [Test]
        public void StringKeyTest()
        {
            var db = new KeyStore<string>(_fileSystem, new Path("strings"));
            for (var i = 0; i < 100000; i++)
            {
                db.Set("asdfasd" + i, "" + i);
            }
            db.Shutdown();
        }

        [Test]
        public void string_test()
        {
            Debug.WriteLine("unlimited key size test");
            var rap = new RaptileDBString(_fileSystem, "longstringkey", false);
            Debug.WriteLine("inserting 10000 ...");
            for (int i = 0; i < 10000; i++)
            {
                rap.Set(String1Kb + i, i.ToString());
            }

            Debug.WriteLine("fetching values...");
            int notfound = 0;
            for (int i = 0; i < 10000; i++)
            {
                string str;
                if (rap.Get(String1Kb + i, out str))
                {
                    if (i.ToString() != str)
                        Assert.Fail("value does not match");
                }
                else
                    notfound++;
            }
            if (notfound > 0)
            {
                Debug.WriteLine("values not found = " + notfound);
                Assert.Fail("values not found = " + notfound);
            }
            else
                Debug.WriteLine("ALL OK");
            rap.Shutdown();
        }
    }
}