defmodule EchoPubSub.Producer.RingBuffer do
  @moduledoc """
  Pure helpers over the producer's fixed-size `:array`. Cursors only grow; a
  cursor maps to slot `rem(cursor, capacity)`, so a write overwrites the message
  `capacity` positions behind it.
  """

  @type t :: :array.array()
  @type cursor :: non_neg_integer()
  @type pending :: :caught_up | {:expired, pos_integer()} | {:messages, [term()]}

  @doc "An empty buffer holding the last `size` messages."
  @spec new(pos_integer()) :: t()
  def new(size), do: :array.new(size)

  @doc "Number of messages the buffer holds."
  @spec capacity(t()) :: pos_integer()
  def capacity(buffer), do: :array.size(buffer)

  @doc "Stores `message` at `cursor`, overwriting the slot's previous message."
  @spec put(t(), cursor(), term()) :: t()
  def put(buffer, cursor, message), do: :array.set(slot(buffer, cursor), message, buffer)

  @doc """
  What a node with `read_cursor` still needs: nothing, the messages from
  `read_cursor` in order, or `{:expired, missed}` once the oldest `missed`
  were overwritten. The messages stop at `write_cursor`, or after `limit` of
  them when that comes first. The limit never changes whether a node expired.
  """
  @spec pending(t(), cursor(), cursor(), pos_integer() | :infinity) :: pending()
  def pending(buffer, read_cursor, write_cursor, limit \\ :infinity) do
    # The read cursor only advances to the write cursor, so lag is never negative.
    lag = write_cursor - read_cursor

    cond do
      lag == 0 ->
        :caught_up

      lag > capacity(buffer) ->
        {:expired, lag - capacity(buffer)}

      true ->
        {:messages,
         Enum.map(read_cursor..last(read_cursor, write_cursor, limit)//1, &read(buffer, &1))}
    end
  end

  defp last(_read_cursor, write_cursor, :infinity), do: write_cursor - 1
  defp last(read_cursor, write_cursor, limit), do: min(write_cursor, read_cursor + limit) - 1

  defp read(buffer, cursor), do: :array.get(slot(buffer, cursor), buffer)

  defp slot(buffer, cursor), do: rem(cursor, capacity(buffer))
end
