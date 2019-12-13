defmodule PartitionMap.SplitLargestStrategy do
  @moduledoc """
  This strategy splits the largest N partitions and assigns one half of each splits to the incoming owner, where each
  owner can have it's own N value.

  For example, you could start with two owners, A and B, where A has one partition and B has three:

  |----------------------------|----------------------------|-----------------------------|----------------------------|
               0(A)                         1(B)                         2(B)                          3(B)

  When we add an owner, C, with two partitions, this strategy will find the current two largest partitions, split them,
  and then assign one half of each to C. In the case where partitions tie for largest, either one will be chosen.

  |----------------------------|----------------------------|--------------|--------------|-------------|--------------|
               0(A)                         1(B)                  2(B)           4(C)          3(B)           5(C)

  In this case, partition 2 and 3 (both owned by B) were the largest. Adding another owner, D, with three partitions
  will split partitions 0 (A), 1 (B) and 5 (C).

  |--------------|-------------|--------------|-------------|--------------|--------------|-------------|-------|------|
        0(A)          7(D)           1(B)          8(D)           2(B)           4(C)          3(B)       5(C)    6(D)

  TODO:
    - allow passing an existing owner to `add_owners/3` in order to change the number of partitions assigned to it
      (should only split partitions not owned by self?)

  """

  import PartitionMap.Util, only: [partitions_with_owners: 1, maybe_default_weights: 1]

  alias PartitionMap.Partition

  @behaviour PartitionMap.Strategy

  # TODO: sanity check args
  @impl true
  @doc false
  def new([owners: owners]) do
    owners = maybe_default_weights(owners)

    partitions =
      owners
      |> Enum.flat_map(fn {owner, num} ->
        Enum.map(1..num, fn i ->
          {owner, i}
        end)
      end)
      |> partitions_with_owners
      |> Enum.map(fn %Partition{owner: {owner, _num}} = partition ->
        %Partition{partition | owner: owner}
      end)
      |> Enum.reverse

    {partitions, owners}
  end

  @impl true
  @doc false
  def add_owners(partitions, additional_owners, owners) do
    additional_owners = maybe_default_weights(additional_owners)

    # we could just choose the largest partitions in a single go, which would be O(n)
    # but this recursion will protect against the possiblity that one of the newly split
    # partitions is still one of the largest (and thus must be re-split), it's O(n^2),
    # but since this algo isn't part of a hot path, i'm not too worried.
    partitions =
      Enum.reduce(additional_owners, partitions, fn {owner, num}, partitions ->
        Enum.reduce(1..num, partitions, fn _i, partitions ->
          %Partition{left: left, right: right} = source_partition =
            partitions
            |> Enum.sort_by(&Partition.size/1)
            |> Enum.reverse
            |> List.first

          new_left =
            source_partition
            |> Partition.size()
            |> div(2)
            |> Kernel.+(left)

          new_partition = %Partition{left: new_left, right: right, owner: owner}
          new_source_partition = %Partition{source_partition | right: new_left}

          partitions
          |> List.delete(source_partition)
          |> List.insert_at(0, new_source_partition)
          |> List.insert_at(0, new_partition)
        end)
      end)

    owners = Map.merge(owners, additional_owners)

    {partitions, owners}
  end
end
