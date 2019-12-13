defmodule PartitionMap.Partition do
  alias IntervalMap.Interval

  defstruct [:left, :right, :id, :owner]

  @type bound :: IntervalMap.bound

  @type t :: %__MODULE__{
    left: bound(),
    right: bound(),
    id: PartitionMap.partition_id(),
    owner: PartitionMap.owner()
  }


  @doc """
  Returns the length of the given `partition`
  """
  @spec size(t) :: integer
  def size(%__MODULE__{left: left, right: right}) do
    right - left
  end

  @doc false
  def overlap?(%__MODULE__{left: left, right: right}, %__MODULE__{left: other_left, right: other_right}) when right <= other_left or other_right <= left, do: false
  def overlap?(%__MODULE__{}, %__MODULE__{}), do: true

  @doc false
  def from_interval(%Interval{left: left, right: right, value: partition}) do
    %__MODULE__{partition | left: left, right: right}
  end

  @doc false
  def marshal(%__MODULE__{id: id, owner: owner}) do
    %__MODULE__{id: id, owner: owner}
  end
end
