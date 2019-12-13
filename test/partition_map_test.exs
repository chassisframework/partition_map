defmodule PartitionMapTest do
  use PartitionMap.Case

  alias PartitionMap.MinimizeKeyMovementAndNumPartitionsStrategy
  alias PartitionMap.SplitLargestStrategy
  alias PartitionMap.Partition

  [
    MinimizeKeyMovementAndNumPartitionsStrategy,
    SplitLargestStrategy
  ]
  |> Enum.each(fn strategy ->
    describe "new/2 #{strategy}" do
      property "produces contiguous maps" do
        forall owners <- owners() do
          unquote(strategy)
          |> PartitionMap.new(owners: owners)
          |> assert_contiguous
        end
      end

      property "produces maps that fill the hash range" do
        forall owners <- owners() do
          unquote(strategy)
          |> PartitionMap.new(owners: owners)
          |> assert_full_range
        end
      end
    end

    describe "add_owners/2 #{strategy}" do
      property "produces contiguous maps" do
        forall owners <- owners() do
          partition_map = unquote(strategy) |> PartitionMap.new(owners: owners)

          forall new_owners <- new_owners(partition_map) do
            partition_map
            |> PartitionMap.add_owners(new_owners)
            |> assert_contiguous
          end
        end
      end

      property "produces maps that fill the hash range" do
        forall owners <- owners() do
          partition_map = unquote(strategy) |> PartitionMap.new(owners: owners)

          forall new_owners <- new_owners(partition_map) do
            partition_map
            |> PartitionMap.add_owners(new_owners)
            |> assert_full_range
          end
        end
      end
    end

    describe "diff/2 #{strategy}" do
      property "applying hunks to the first map results in the second map" do
        forall owners <- owners() do
          original_map = unquote(strategy) |> PartitionMap.new(owners: owners)

          forall new_owners <- new_owners(original_map) do
            new_map = PartitionMap.add_owners(original_map, new_owners)

            diff = PartitionMap.diff(original_map, new_map)

            original_map
            |> TestHelper.apply_diff(diff)
            |> Enum.zip(PartitionMap.to_list(new_map))
            |> Enum.each(fn
              {%Partition{id: id, left: left, right: right}, %Partition{id: id, left: left, right: right}} ->
                true

              _ ->
                flunk "mismatched partitions"
            end)

            true
          end
        end
      end
    end
  end)
end
