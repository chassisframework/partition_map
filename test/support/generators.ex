defmodule PartitionMap.Generators do
  use PropCheck

  def owners do
    oneof([
      owners_list(),
      owners_map()
    ])
  end

  def new_owners(%PartitionMap{} = partition_map) do
    current_owners = PartitionMap.owners(partition_map)

    oneof([
      owners_list(current_owners),
      owners_map(current_owners)
    ])
  end

  def owners_list(current_things \\ []) do
    unique_list_of(owner_name(), current_things, fn
      {owner_name, _weight} -> owner_name
      owner_name -> owner_name
    end)
  end

  def owners_map(current_owners \\ []) do
    let list_with_weights <-
      unique_list_of(owner_name_with_weight(), current_owners, fn
        {owner_name, _weight} -> owner_name
        owner_name -> owner_name
      end) do
      Enum.into(list_with_weights, %{})
    end
  end

  defp owner_name_with_weight do
    let [name <- owner_name(),
         number <- weight()] do
      {name, number}
    end
  end

  defp weight do
    pos_integer()
  end

  defp owner_name do
    atom()
  end

  defp unique_list_of(generator, current_things, unique_by) do
    sized(size, do_unique_list_of(size, generator, unique_by, [], current_things))
  end

  defp do_unique_list_of(0, _generator, _unique_by, things, _current_thing) do
    things
  end

  defp do_unique_list_of(size, generator, unique_by, things, current_things) do
    let thing <- new_unique_thing(generator, current_things, unique_by) do
      do_unique_list_of(size - 1, generator, unique_by, [thing | things], [thing | current_things])
    end
  end

  # TODO: this is really slow, O(N)
  defp new_unique_thing(generator, current, unique_by) do
    such_that thing <- generator, when:
    !Enum.any?(current, fn current_thing ->
      unique_by.(current_thing) == unique_by.(thing)
    end)
  end
end
