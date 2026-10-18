defmodule EchoPubSub.Producer.Registration do
  @moduledoc """
  Registers a remote worker before its first batch: `{:register, node, missed_writes?}`
  when the producer has no cursor for it, `{:resume, node}` when only its worker
  restarted.
  Runs in a task; `finish/5` folds the reply into the producer state.
  """

  alias EchoPubSub.Producer.Remote
  alias EchoPubSub.Producer.State
  alias EchoPubSub.Producer.Timers

  @doc "Whether a registration task for `node` is running (not a failed one waiting to retry)."
  @spec running?(State.t(), node()) :: boolean()
  def running?(state, node), do: State.registration_running?(state, node)

  @doc "Whether `pid` still needs registering: never registered, or its worker restarted since."
  @spec needed?(State.t(), pid(), node()) :: boolean()
  def needed?(state, pid, node), do: Map.get(state.workers, node) != pid

  @doc "Starts the registration task for `pid` on `node`."
  @spec start(State.t(), pid(), node()) :: State.t()
  def start(state, pid, node) do
    start = start_cursor(state, node)
    message = message(state, node, start)
    timeout = state.call_timeout
    supervisor = Module.concat(state.group, TaskSupervisor)

    task =
      Task.Supervisor.async_nolink(supervisor, fn -> Remote.register(pid, message, timeout) end)

    State.track_registration(state, node, task.ref, start, pid)
  end

  @doc """
  Applies the task's result. On `:ok` the node is registered and keeps any cursor
  it had, so a restarted worker still gets what its old one missed. On `:error`
  it waits for the retry flush with the same start cursor.
  """
  @spec finish(State.t(), node(), State.cursor(), pid(), :ok | :error) :: State.t()
  def finish(state, node, start, pid, :ok) do
    state |> State.registered(node, pid, start) |> Timers.follow_up(node)
  end

  def finish(state, node, start, pid, :error) do
    state |> State.registration_failed(node, start, pid) |> Timers.retry()
  end

  # Writes below the start were flushed before this registration and never reach
  # the node, so the flag tells its worker to send cursor_expired.
  defp message(state, node, start) do
    if known?(state, node),
      do: {:resume, node()},
      else: {:register, node(), missed_writes?(start)}
  end

  defp known?(state, node), do: Map.has_key?(state.read_cursors, node)

  # Possibly missed: those writes are never sent, but the node may already hold
  # their changes, e.g. from a state load after they were written.
  defp missed_writes?(start), do: start > 0

  # On the first attempt the node joins the current batch window: it starts at
  # the cursor the last flush ended at (the local node's cursor), so it gets every
  # write not yet flushed, as it would have had it been a member when that flush
  # ran. A retry keeps that start, so writes flushed while earlier attempts were
  # failing are still delivered.
  defp start_cursor(state, node) do
    State.retry_start(state, node) || Map.get(state.read_cursors, node(), 0)
  end
end
