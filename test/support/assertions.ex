defmodule PartitionMap.Assertions do

  import ExUnit.Assertions
  import PartitionMap.Util, only: [sum_by: 2, range_limits: 0]

  alias PartitionMap.Partition

  def assert_contiguous(%PartitionMap{interval_map: interval_map}) do
    assert IntervalMap.contiguous?(interval_map)
  end

  def assert_full_range(%PartitionMap{} = partition_map) do
    partitions = PartitionMap.to_list(partition_map)

    %Partition{left: left} = List.first(partitions)
    %Partition{right: right} = List.last(partitions)

    {range_start, range_end} = range_limits()
    assert left == range_start
    assert right == range_end
  end

  def assert_relative_weights(%PartitionMap{} = partition_map, owners) do
    total_weight = sum_by(owners, fn {_name, weight} -> weight end)

    expected_weights =
      Enum.into(owners, %{}, fn {name, weight} ->
        {name, weight / total_weight}
      end)

    partition_map
    |> PartitionMap.calculate_relative_owner_weights
    |> Enum.each(fn {name, weight} ->
      expected_weights
      |> Map.get(name)
      |> assert_in_delta(weight, 0.00000001)
    end)

    true
  end
end
