defmodule TestHelper do
  alias PartitionMap.Partition
  alias PartitionMap.Diff
  alias PartitionMap.Diff.Hunk

  def apply_diff(%PartitionMap{} = partition_map, %Diff{hunks: hunks, deleted_ids: deleted_ids, added_ids: added_ids}) do
    partitions = PartitionMap.to_list(partition_map)

    partitions_by_id = Enum.into(partitions, %{}, fn %Partition{id: id} = partition -> {id, partition} end)

    shrank_partitions =
      hunks
      |> Enum.reject(fn %Hunk{from_id: from_id} -> Enum.member?(deleted_ids, from_id) end)
      |> Enum.group_by(fn %Hunk{from_id: from_id} -> from_id end)
      |> Enum.map(fn {id, hunks} -> {Map.get(partitions_by_id, id), hunks} end)
      |> Enum.map(fn {%Partition{left: left} = partition, hunks} ->
      {from_left, from_right} = Enum.split_with(hunks, fn %Hunk{left: hunk_left} -> hunk_left < left end)

      [stitch_contiguous_hunks(from_left), stitch_contiguous_hunks(from_right)]
      |> Enum.reject(&is_nil/1)
      |> Enum.reduce(partition, fn
        {left, right}, %Partition{left: left} = partition ->
          %Partition{partition | left: right}
        {left, right}, %Partition{right: right} = partition ->
          %Partition{partition | right: left}
      end)
    end)

    {new_hunks, growth_hunks} =
        hunks
        |> Enum.group_by(fn %Hunk{to_id: to_id} -> to_id end)
        |> Enum.split_with(fn {id, _hunks} -> Enum.member?(added_ids, id) end)

    new_partitions =
      new_hunks
      |> Enum.map(fn {id, hunks} -> {id, stitch_contiguous_hunks(hunks)} end)
      |> Enum.map(fn {id, {left, right}} -> %Partition{id: id, left: left, right: right} end)

    grew_partitions =
      growth_hunks
      |> Enum.map(fn {id, hunks} -> {Map.get(partitions_by_id, id), hunks} end)
      |> Enum.map(fn {%Partition{left: left} = partition, hunks} ->
      {from_left, from_right} = Enum.split_with(hunks, fn %Hunk{left: hunk_left} -> hunk_left < left end)

      [stitch_contiguous_hunks(from_left), stitch_contiguous_hunks(from_right)]
      |> Enum.reject(&is_nil/1)
      |> Enum.reduce(partition, fn
        {left, right}, %Partition{left: right} = partition ->
          %Partition{partition | left: left}
        {left, right}, %Partition{right: left} = partition ->
          %Partition{partition | right: right}
      end)
    end)

      changed_ids = Enum.flat_map(hunks, fn %Hunk{from_id: from_id, to_id: to_id} -> [from_id, to_id] end)
      unchanged_partitions = Enum.reject(partitions, fn %Partition{id: id} -> Enum.member?(changed_ids, id) end)

      unchanged_partitions ++ shrank_partitions ++ new_partitions ++ grew_partitions
      |> Enum.sort_by(fn %Partition{left: left} -> left end)
  end

  defp stitch_contiguous_hunks([]), do: nil

  defp stitch_contiguous_hunks([%Hunk{left: left, right: stitch_point} = hunk, %Hunk{left: stitch_point, right: right} | rest]) do
    stitch_contiguous_hunks([%Hunk{hunk | left: left, right: right} | rest])
  end
  defp stitch_contiguous_hunks([%Hunk{left: left, right: right}]), do: {left, right}
end

ExUnit.start()
