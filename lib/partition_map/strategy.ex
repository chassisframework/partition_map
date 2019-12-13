defmodule PartitionMap.Strategy do
  @type partitions :: PartitionMap.partitions()
  @type strategy_private_state :: any

  @callback new(PartitionMap.strategy_args()) :: {partitions, strategy_private_state}
  @callback add_owners(partitions, PartitionMap.owners(), strategy_private_state) :: {partitions, strategy_private_state}
  # @callback digest_key(key) :: term
end
