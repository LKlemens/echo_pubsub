defmodule EchoPubSub.Producer.Shutdown do
  @moduledoc """
  Sends every registered remote node what it still needs when the producer stops
  gracefully, so writes that already returned `:ok` but were not flushed yet are
  not lost with the buffer.

  Best effort: one call per node with its whole backlog (no batch limit), all at
  once, and the producer waits at most `call_timeout` in total. A batch already in
  flight is sent again, which at-least-once delivery allows. A node that cannot be
  reached in time keeps what it missed on the floor: the buffer dies with the
  producer, so a producer that starts again under the same node name tells it
  `{:cursor_expired, node}`.
  """

  alias EchoPubSub.Producer.Remote
  alias EchoPubSub.Producer.RingBuffer
  alias EchoPubSub.Producer.State

  @doc "Whether `reason` is a graceful stop, the only one worth waiting on peers for."
  @spec graceful?(term()) :: boolean()
  def graceful?(reason), do: reason in [:normal, :shutdown] or match?({:shutdown, _}, reason)

  @doc "Delivers every registered node's pending messages, waiting at most `call_timeout`. Never raises."
  @spec flush(State.t()) :: :ok
  def flush(state) do
    supervisor = Module.concat(state.group, TaskSupervisor)

    tasks =
      for {node, pid} <- state.workers,
          node != node(),
          {:messages, messages} <- [pending(state, node)] do
        Task.Supervisor.async_nolink(supervisor, fn ->
          Remote.deliver(pid, {:messages, messages}, state.call_timeout)
        end)
      end

    tasks
    |> Task.yield_many(state.call_timeout)
    |> Enum.each(fn {task, result} -> result || Task.shutdown(task, :brutal_kill) end)
  catch
    # The task supervisor may already be gone; nothing here may block the stop.
    _kind, _reason -> :ok
  end

  # A node without a read cursor never finished registering: it is owed nothing yet.
  defp pending(state, node) do
    case Map.fetch(state.read_cursors, node) do
      {:ok, cursor} -> RingBuffer.pending(state.buffer, cursor, state.write_cursor)
      :error -> :caught_up
    end
  end
end
