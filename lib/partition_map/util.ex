defmodule PartitionMap.Util do
  @moduledoc false

  alias PartitionMap.Partition

  @hash_range :math.pow(2, 32) |> trunc()
  @doc false
  def hash_range, do: @hash_range

  # -1 because phash2 returns a hash within 0..@phash_range - 1 and IntervalMap's intervals are left-open, right-closed
  @min_range -1
  @max_range @hash_range - 1
  @doc false
  def range_limits, do: {@min_range, @max_range}

  @doc false
  def partitions_with_owners(owners) do
    owners = maybe_default_weights(owners)

    {range_start, _range_end} = range_limits()

    owners
    |> calculate_num_keys_by_owner()
    |> Enum.sort_by(fn {owner, _num} -> owner end) # deterministic ordering
    |> Enum.reduce({range_start, []}, fn {owner, num_keys}, {left, partitions} ->
      right = left + num_keys

      partition =
        %Partition{
          left: left,
          right: right,
          owner: owner
        }

      {right, [partition | partitions]}
    end)
    |> elem(1)
  end

  # instead of getting into the weeds of trying to explicitly track rounding errors from round(),
  # we just assign the spare under/over keys to the partition that sat out the calculation
  @doc false
  def calculate_num_keys_by_owner(owners) do
    weight_sum = sum_by(owners, fn {_owner, weight} -> weight end)

    first_owner =
      owners
      |> Map.keys
      |> List.first

    owners = Map.delete(owners, first_owner)

    num_keys_by_owner =
      Map.new(owners, fn {owner, weight} ->
        keyspace_fraction = weight / weight_sum
        num_keys = hash_range() * keyspace_fraction |> round()

        {owner, num_keys}
      end)

    num_assigned_keys = sum_by(num_keys_by_owner, fn {_owner, keys} -> keys end)

    Map.put(num_keys_by_owner, first_owner, hash_range() - num_assigned_keys)
  end

  @doc false
  def sum_by(enum, func) do
    Enum.reduce(enum, 0, fn i, sum -> func.(i) + sum end)
  end

  @doc false
  def hash(term) do
    :erlang.phash2(term, @hash_range)
  end

  @doc false
  def deterministic_integer_for_colors(term) do
    trunc(5 * hash(term) / @hash_range)
  end

  @doc false
  def maybe_default_weights(owners) when is_list(owners) do
    Enum.into(owners, %{}, & {&1, 1})
  end
  def maybe_default_weights(owners) when is_map(owners), do: owners
end
