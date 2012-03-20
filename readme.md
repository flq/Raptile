## Raptile - A fork from the raptor DB

[Raptor DB][1] is a project from [Mehdi Gholam][2] and is a Key-Value store that is persisted onto disk.
What I was missing in the original implementation was

* Consistency in programming conventions. call me nitpick, but I need my code-base to follow some set of guidelines so I can orient myself more easily.
* Tests. There are a couple of tests, but this doesn't go far enough for me.

Additionally, Raptile has a different focus. Things that already changed are e.g.

* support that was started for duplicate keys in RaptorDB has been removed. 
* Usage of the file system has been abstracted through the lovely File system abstraction [OpenFileSystem][4]. This allows for very simple in-memory testing.

What you can expect in the future is the ability to define secondary indices to quickly retrieve groups of related objects.

__Hence__, while you can consider this to be a fork of Raptor DB, it is unlikely that I will pull code updates done to Raptor DB.

## Licensing 

RaptorDB follows the [Apache 2.0 license][3], Raptile will stay there as well

  [1]: http://raptordbkv.codeplex.com/
  [2]: http://www.codeproject.com/script/Membership/View.aspx?mid=151481
  [3]: http://raptordbkv.codeplex.com/license
  [4]: https://github.com/openrasta/openfilesystem