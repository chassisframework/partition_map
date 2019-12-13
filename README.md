# PartitionMap

*WORK IN PROGRESS*

PartitionMap maps arbitrary terms to a set of contiguous and dynamic (but deterministic) partitions. It's intended as a hash table for distributed systems.

The general goal is to assign every possible term to a set of known buckets, without the O(N) size requirement of a Map.

You can think of PartitionMap like [Consistent Hashing](https://en.wikipedia.org/wiki/Consistent_hashing) but with a couple of key advantages:
  - dynamic number of partitions
  - dynamically sized partitions
  - pluggable slicing strategies for application-specific optimizations
  - doesn't conflate partition assignment with replication policies

There's a great article by @slfritchie on the topic: [A Critique of Resizable Hash Tables: Riak Core & Random Slicing](https://www.infoq.com/articles/dynamo-riak-random-slicing)

PartitionMap takes heavy inspiration and uses the "CutShift" algorithm from ["Random slicing: Efficient and scalable data placement for large-scale storage systems."](docs/miranda-tos14.pdf)

## Internals

PartitionMap works by hashing the given term into a fixed-size range of integer keys, the hash lands inside one of many contiguous intervals of integers (partitions).

Each partition belongs to an "owner", each owner owns a configurable proportion of the total hash space.

A graphical example:

```
 owner1     o2           o1           o1       o2        o1             o2
   v        v            v            v        v         v              v
<- p1 ->|<- p2 ->|<----- p3 ----->|<- p4 ->|<- p5 ->|<-- p6 -->|<------ p7 ------>|
|-------|--------|----------------|--------|--------|----------|------------------|
^  ^        ^        ^       ^           ^               ^                        ^
0  |        |        |       |           |               |                   2**32 (phash2)
   |        |        |       |           |               |
 "abcd"  <<1,2>>    "z"  %{a: 123}      500        {:ok, [1, 2, 3]}
```

So, the terms `"z"` and `%{a: 123}` are both mapped to the same partition.

## Installation

The package can be installed by adding `partition_map` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:partition_map, "~> 0.1.0"}
  ]
end
```

The docs can be found at [https://hexdocs.pm/partition_map](https://hexdocs.pm/partition_map).
