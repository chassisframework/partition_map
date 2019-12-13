defmodule PartitionMap.MinimizeKeyMovementAndNumPartitionsStrategyTest do
  use PartitionMap.Case

  alias PartitionMap.Partition
  alias PartitionMap.MinimizeKeyMovementAndNumPartitionsStrategy

  describe "new/1" do
    test "sanity check" do
      #
      # |--------------------------------------|--------------------------------------|--------------------------------------|
      #                    2(a)                                   1(b)                                   0(c)
      #
      assert [
        %Partition{id: 2, left: -1, owner: :a, right: 1431655764},
        %Partition{id: 1, left: 1431655764, owner: :b, right: 2863311529},
        %Partition{id: 0, left: 2863311529, owner: :c, right: 4294967295}
      ] =
        MinimizeKeyMovementAndNumPartitionsStrategy
        |> PartitionMap.new(owners: [:a, :b, :c])
        |> PartitionMap.to_list()
    end

    property "produces maps with correct relative weighting" do
      forall owners <- owners_map() do
        MinimizeKeyMovementAndNumPartitionsStrategy
        |> PartitionMap.new(owners: owners)
        |> assert_relative_weights(owners)
      end
    end
  end

  describe "add_owners/2" do
    test "sanity check" do
      #
      # |------------------|-------------------|------------------|-------------------|-------------------|------------------|
      #          2(a)               5(d)                4(e)               1(b)                0(c)                3(e)
      #
      assert [
        %PartitionMap.Partition{id: 2, left: -1, owner: :a, right: 715827882},
        %PartitionMap.Partition{id: 5, left: 715827882, owner: :d, right: 1431655765},
        %PartitionMap.Partition{id: 4, left: 1431655765, owner: :e, right: 2147483646},
        %PartitionMap.Partition{id: 1, left: 2147483646, owner: :b, right: 2863311529},
        %PartitionMap.Partition{id: 0, left: 2863311529, owner: :c, right: 3579139411},
        %PartitionMap.Partition{id: 3, left: 3579139411, owner: :e, right: 4294967295}
      ] =
        MinimizeKeyMovementAndNumPartitionsStrategy
        |> PartitionMap.new(owners: [:a, :b, :c])
        |> PartitionMap.add_owners(%{d: 1, e: 2})
        |> PartitionMap.to_list
    end

    property "produces maps with correct relative weighting" do
      forall owners <- owners() do
        partition_map = PartitionMap.new(MinimizeKeyMovementAndNumPartitionsStrategy, owners: owners)

        forall new_owners <- new_owners(partition_map) do
          owners =
            owners
            |> Enum.concat(new_owners)
            |> Enum.map(fn
              {name, weight} ->
                {name, weight}

              name ->
                {name, 1}
              end)

          partition_map
          |> PartitionMap.add_owners(new_owners)
          |> assert_relative_weights(owners)
        end
      end
    end
  end
end
