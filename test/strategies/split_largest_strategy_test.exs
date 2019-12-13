defmodule PartitionMap.SplitLargestStrategyTest do
  use PartitionMap.Case

  alias PartitionMap.Partition
  alias PartitionMap.SplitLargestStrategy
  alias PartitionMap.Util


  describe "new/2" do
    property "produces maps with equal partitions, correctly owned" do
      forall owners <- owners() do
        partitions =
          SplitLargestStrategy
          |> PartitionMap.new(owners: owners)
          |> PartitionMap.to_list

        num_per_owner =
          partitions
          |> Enum.group_by(fn %Partition{owner: owner} -> owner end)
          |> Enum.into(%{}, fn {owner, partitions} -> {owner, length(partitions)} end)

        expected_num_per_owner = Util.maybe_default_weights(owners)

        num_per_owner == expected_num_per_owner
      end
    end
  end
end
