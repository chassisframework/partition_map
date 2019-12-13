defmodule PartitionMap do
  @moduledoc """
  PartitionMap maps arbitrary terms to a set of contiguous and dynamic (but deterministic) partitions. It's intended as a hash table for distributed systems.

  The general goal is to assign every possible term to a set of known buckets, without the O(N) size requirement of a Map.

  You can think of PartitionMap like [Consistent Hashing](https://en.wikipedia.org/wiki/Consistent_hashing) but with a couple of key advantages:
    - dynamic number of partitions
    - dynamically sized partitions
    - pluggable slicing strategies for application-specific optimizations
    - doesn't conflate partition assignment with replication policies

  There's a great article by @slfritchie on the topic: [A Critique of Resizable Hash Tables: Riak Core & Random Slicing](https://www.infoq.com/articles/dynamo-riak-random-slicing)

  PartitionMap works by hashing the given term into a fixed-size range of integer keys, the hash lands inside one of many contiguous intervals of integers (partitions).

  Each partition belongs to an "owner", each owner owns a configurable proportion of the total hash space.

  A graphical example:

    owner 1    o2           o1           o1       o2        o1             o2
      v        v            v            v        v         v              v
  <- p1 ->|<- p2 ->|<----- p3 ----->|<- p4 ->|<- p5 ->|<-- p6 -->|<------ p7 ------>|
  |-------|--------|----------------|--------|--------|----------|------------------|
  ^  ^        ^        ^       ^           ^               ^                        ^
  0  |        |        |       |           |               |                   2**32 (phash2)
     |        |        |       |           |               |
   "abcd"  <<1,2>>    "z"  %{a: 123}      500        {:ok, [1, 2, 3]}

  So, the terms `"z"` and `%{a: 123}` are both mapped to the same partition.

  PartitionMap takes heavy inspiration and uses the "CutShift" algorithm from:
    ["Random slicing: Efficient and scalable data placement for large-scale storage systems."](docs/miranda-tos14.pdf)
    Alberto Miranda, Sascha Effert, Yangwook Kang, Ethan L. Miller, Ivan Popov, Andre Brinkmann, TomFriedetzky, and Toni Cortes. 2014.
    ACM Trans. Storage 10, 3, Article 9 (July 2014), 35 pages.
    ftp://ftp.cse.ucsc.edu/pub/darrell/miranda-tos14.pdf
  """

  alias PartitionMap.Partition
  alias PartitionMap.Diff

  alias PartitionMap.Util
  import PartitionMap.Util, only: [sum_by: 2, hash_range: 0, deterministic_integer_for_colors: 1]

  defstruct [
    strategy: :unset,
    interval_map: IntervalMap.new(),
    private: :unset, # the strategy's private state
    next_id: 0
  ]

  @type key :: any
  @type partition_id :: integer
  @type partitions :: [Partition.t()]

  @type strategy :: module
  @type strategy_args :: list

  @type weight :: number
  @type owner :: any
  @type owner_names :: [owner]
  @type owners_with_weights :: %{required(owner) => weight}
  @type owners :: owner_names | owners_with_weights

  @type t :: %__MODULE__{
    strategy: strategy,
    interval_map: IntervalMap.t(),
    private: atom,
    next_id: integer
  }

  @visualization_delimeter "|"

  @doc """
    Creates a new PartitionMap with the specified strategy and strategy arguments.

    iex> PartitionMap.new(PartitionMap.SplitLargestN, n: 8, owners: [:a, :b, :c, :d])

    # => %PartitionMap{}

  """
  @spec new(strategy, strategy_args) :: t
  def new(strategy, strategy_args) when is_atom(strategy) and is_list(strategy_args) do
    {partitions, private} = apply(strategy, :new, [strategy_args])

    {partitions, next_id} = assign_new_ids(partitions, 0)

    interval_map = partitions_to_interval_map(partitions)

    %__MODULE__{interval_map: interval_map, strategy: strategy, private: private, next_id: next_id}
  end

  @doc """
  """
  @spec get(t, key) :: Partition.t()
  def get(%__MODULE__{} = partition_map, term) do
    get_with_digested_key(partition_map, digest_key(term))
  end

  @doc """
  """
  @spec get_with_digested_key(t, key) :: Partition.t()
  def get_with_digested_key(%__MODULE__{interval_map: interval_map}, term) do
    interval_map
    |> IntervalMap.get(term)
    |> Partition.from_interval
  end

  defdelegate digest_key(key), to: Util, as: :hash

  @doc """
  Adds a number of owners to the given `partition_map`, partitions are then assigned to the new owners
  using the partition_map's strategy.
  """
  @spec add_owners(t, owners) :: t
  def add_owners(%__MODULE__{strategy: strategy, private: private, next_id: next_id} = partition_map, owners) do
    partitions = to_list(partition_map)

    {partitions, private} = apply(strategy, :add_owners, [partitions, owners, private])

    {partitions, next_id} = assign_new_ids(partitions, next_id)

    interval_map = partitions_to_interval_map(partitions)

    %__MODULE__{partition_map | interval_map: interval_map, private: private, next_id: next_id}
  end

  @doc """
  Returns a list of differences between `partition_map` and `other_partition_map` as a `Diff`.
  """
  defdelegate diff(partition_map, other_partition_map), to: Diff

  @doc """
  Converts the given `partition_map` to a list.
  """
  @spec to_list(t) :: partitions
  def to_list(%__MODULE__{interval_map: interval_map}) do
    interval_map
    |> IntervalMap.to_list
    |> Enum.map(&Partition.from_interval/1)
  end

  @doc """
  Returns a list of owners in the given `partition_map`
  """
  @spec owners(t) :: owner_names
  def owners(%__MODULE__{} = partition_map) do
    partition_map
    |> to_list
    |> Enum.map(fn %Partition{owner: owner} -> owner end)
  end

  @doc """
  The size of the given partition
  """
  defdelegate size(partition), to: Partition

  # for debugging/testing
  @doc false
  @spec calculate_relative_owner_weights(t) :: owners_with_weights
  def calculate_relative_owner_weights(%__MODULE__{} = partition_map) do
    partition_map
    |> to_list
    |> Enum.group_by(fn %Partition{owner: owner} -> owner end)
    |> Enum.into(%{}, fn {owner, partitions} ->
      num_keys = sum_by(partitions, &Partition.size/1)
      weight = num_keys / hash_range()
      {owner, weight}
    end)
  end

  @doc """
  Visually inspect a PartionMap.

  The wider you make your terminal, the more accurate it'll be.
  """
  def inspect(%__MODULE__{} = partition_map, width \\ iex_columns()) do
    partitions = to_list(partition_map)

    total_pips = width - 1 # due to initial delimeter

    # owner colors are calculated in a deterministic manner to ensure that their colors stay the same
    # between invocations, so we can see a clear timeline
    owner_colors =
      partitions
      |> Enum.map(fn %Partition{owner: owner} -> owner end)
      |> Enum.uniq
      |> Enum.into(%{}, fn owner ->
        r = deterministic_integer_for_colors({owner, :red})
        g = deterministic_integer_for_colors({owner, :green})
        b = deterministic_integer_for_colors({owner, :blue})
        {owner, IO.ANSI.color(r, g, b)}
      end)

    num_keys_per_pip = hash_range() / total_pips

    # this looks convoluted, but it avoids cascading round() errors by calculating the absolute positions
    # first, then rounding. (rather than adding pre-rounded numbers in series)
    {sections, tags} =
      Enum.reduce(partitions, {[], 0}, fn partition, {keys_so_far, sum} ->
        sum = sum + Partition.size(partition)
        {[sum | keys_so_far], sum}
      end)
      |> elem(0)
      |> Enum.reverse
      |> Enum.map(fn num_keys ->
        num_keys / num_keys_per_pip |> round()
      end)
      |> Enum.reduce({[], 0}, fn absolute_pip_index, {pip_lengths, last_pip_index} ->
        {[absolute_pip_index - last_pip_index | pip_lengths], absolute_pip_index}
      end)
      |> elem(0)
      |> Enum.reverse
      |> Enum.map(&Kernel.-(&1, 1)) # subtract one from each length for its delimeter
      |> Enum.zip(partitions)
      |> Enum.map(fn {num_pips, %Partition{owner: owner, id: id}} ->
        owner_color = Map.get(owner_colors, owner)

        section = String.pad_leading("", num_pips, "-")

        tag =
          "#{id}(#{owner})"
          |> String.pad_leading(div(num_pips, 2) + 3, " ")
          |> String.pad_trailing(num_pips + 1, " ")

        {
          [owner_color, section, IO.ANSI.reset, @visualization_delimeter],
          [owner_color, tag, IO.ANSI.reset]
        }
      end)
      |> Enum.unzip

    [@visualization_delimeter, sections, "\n", tags] |> IO.puts

    partition_map
  end

  defp partitions_to_interval_map(partitions) do
    Enum.reduce(partitions, IntervalMap.new(), fn %Partition{left: left, right: right} = partition, interval_map ->
      IntervalMap.put(interval_map, {left, right}, Partition.marshal(partition))
    end)
  end

  defp assign_new_ids(partitions, next_id) do
    {without_ids, with_ids} = Enum.split_with(partitions, fn %Partition{id: id} -> is_nil(id) end)

    newly_assigned =
      without_ids
      |> Enum.with_index
      |> Enum.map(fn {partition, index} -> %Partition{partition | id: next_id + index} end)

    {newly_assigned ++ with_ids, next_id + length(newly_assigned)}
  end

  defp iex_columns do
    {:ok, columns} = :io.columns
    columns
  end
end
