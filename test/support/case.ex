defmodule PartitionMap.Case do
  defmacro __using__(_opts) do
    quote do
      use ExUnit.Case
      use PropCheck

      import PartitionMap.Generators
      import PartitionMap.Assertions
    end
  end
end
