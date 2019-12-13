defmodule PartitionMap.MinimizeKeyMovementAndNumPartitionsStrategy do
  @moduledoc """
    This strategy attempts the following:
    - Minimize the number of total partitions.
    - Minimize the number of keys moved between existing owners when adding a new owner, that is,
      keys should only move from existing owners to new owners.

    This strategy allows you to weight each owner, to allow each owner to take differing proportions
    of the keyspace.

    Weights are relative, the sum of the weights represents the whole keyspace, e.g.
      node_1 -> 1
      node_2 -> 2
      node_3 -> 1

      So node_2 would own half of the keyspace, and nodes 2 and 3 each own a quarter.
  """

  alias PartitionMap.Partition

  import PartitionMap.Util, only: [partitions_with_owners: 1,
                                   calculate_num_keys_by_owner: 1,
                                   maybe_default_weights: 1]

  @behaviour PartitionMap.Strategy

  # `owners` is a map of owners to weights
  @impl true
  @doc false
  def new([owners: owners]) when is_map(owners) and map_size(owners) == 0, do: raise "empty map, fix this error message"
  def new([owners: []]), do: raise "empty list, fix this error message"

  def new([owners: owners]) when is_map(owners) or is_list(owners) do
    owners = maybe_default_weights(owners)

    {partitions_with_owners(owners), owners}
  end

  @impl true
  @doc false
  def add_owners(partitions, additional_owners, owners) when is_map(additional_owners) or is_list(additional_owners) do
    additional_owners = maybe_default_weights(additional_owners)
    new_owners = Map.merge(owners, additional_owners)

    new_num_keys_by_owner =
      owners
      |> Map.merge(additional_owners)
      |> calculate_num_keys_by_owner()

    num_keys_to_shrink_by_owner =
      owners
      |> calculate_num_keys_by_owner()
      |> Enum.into(%{}, fn {owner, old_num} ->
        new_num = Map.get(new_num_keys_by_owner, owner)
        {owner, old_num - new_num}
      end)

    {partitions, gaps, _num_keys_to_shrink_by_owner} =
      Enum.reduce(partitions, {[], [], num_keys_to_shrink_by_owner}, fn %Partition{owner: owner, left: left, right: right} = partition, {partitions, gaps, num_keys_to_shrink_by_owner} ->
        num_keys_to_shrink = Map.get(num_keys_to_shrink_by_owner, owner)

        if num_keys_to_shrink > 0 do
          partition_size = Partition.size(partition)

          if partition_size <= num_keys_to_shrink do
            # consume the entire partition
            {partitions, [{left, right} | gaps], Map.put(num_keys_to_shrink_by_owner, owner, num_keys_to_shrink - partition_size)}
          else
            if shrink_from_left?(partition, gaps) do
              new_left = left + num_keys_to_shrink
              partition = %Partition{partition | left: new_left}

              {[partition | partitions], [{left, new_left} | gaps], Map.put(num_keys_to_shrink_by_owner, owner, 0)}
            else
              new_right = right - num_keys_to_shrink
              partition = %Partition{partition | right: new_right}

              {[partition | partitions], [{new_right, right} | gaps], Map.put(num_keys_to_shrink_by_owner, owner, 0)}
            end
          end
        else
          {[partition | partitions], gaps, num_keys_to_shrink_by_owner}
        end
      end)


    gaps =
      gaps
      |> Enum.reverse
      |> combine_gaps()

    new_partitions =
      new_num_keys_by_owner
      |> Map.take(Map.keys(additional_owners))
      |> Map.to_list
      |> assign_gaps(gaps)

    {new_partitions ++ partitions, new_owners}
  end

  # for testing
  @doc false
  def owners(%PartitionMap{private: owners}) do
    owners
  end

  defp shrink_from_left?(%Partition{left: last_gap_right}, [{_last_gap_left, last_gap_right} | _gaps]), do: true
  defp shrink_from_left?(_partition, _gaps), do: false

  defp combine_gaps([{left, right}, {right, other_right} | rest]), do: combine_gaps([{left, other_right} | rest])
  defp combine_gaps([disjoint_gap, next_gap | rest]), do: [disjoint_gap | combine_gaps([next_gap | rest])]
  defp combine_gaps([last_gap]), do: [last_gap]


  defp assign_gaps(new_owners, gaps, new_partitions \\ [])
  defp assign_gaps([{new_owner, keys_remaining} | owners_rest], [{left, right} | gaps_rest], new_partitions) when keys_remaining < right - left do
    new_gap_left = left + keys_remaining
    partition = %Partition{left: left, right: new_gap_left, owner: new_owner}
    assign_gaps(owners_rest, [{new_gap_left, right} | gaps_rest], [partition | new_partitions])
  end

  defp assign_gaps([{new_owner, keys_remaining} | owners_rest], [{left, right} | gaps_rest], new_partitions) when keys_remaining == right - left do
    partition = %Partition{left: left, right: right, owner: new_owner}
    assign_gaps(owners_rest,  gaps_rest, [partition | new_partitions])
  end

  defp assign_gaps([{new_owner, keys_remaining} | owners_rest], [{left, right} | gaps_rest], new_partitions) when keys_remaining > right - left do
    partition = %Partition{left: left, right: right, owner: new_owner}
    assign_gaps([{new_owner, keys_remaining - (right - left)} | owners_rest], gaps_rest, [partition | new_partitions])
  end

  defp assign_gaps([], [], new_partitions), do: new_partitions
end
