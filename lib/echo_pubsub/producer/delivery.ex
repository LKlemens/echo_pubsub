defmodule EchoPubSub.Producer.Delivery do
  @moduledoc """
  Sends a registered node what it still needs, in a task. The node's cursor moves
  to the batch's target only when its worker acks, so a failed batch is resent.

  A failed call may still sit in a hung worker's mailbox, so the node is probed
  with `:ping` before the batch or registration is resent: the worker answers in
  mailbox order, after handling that call. Without it, every retry would queue
  another copy.
  """

  alias EchoPubSub.Producer.Remote
  alias EchoPubSub.Producer.State
  alias EchoPubSub.Producer.Telemetry
  alias EchoPubSub.Producer.Timers

  @doc "Whether a batch to `node` is in flight."
  @spec running?(State.t(), node()) :: boolean()
  def running?(state, node), do: Map.has_key?(state.in_flight, node)

  @doc "Starts a delivery task to `pid` unless `node` is caught up. Only registered nodes get here."
  @spec start(State.t(), pid(), node()) :: State.t()
  def start(state, pid, node) do
    case State.pending(state, node) do
      :caught_up -> state
      batch -> start_task(state, pid, node, batch)
    end
  end

  @doc """
  Pings `pid` in a task after `node` failed a registration or delivery; the
  next attempt waits for its answer. Tracked as the node's call in flight.
  """
  @spec probe(State.t(), pid(), node()) :: State.t()
  def probe(state, pid, node) do
    timeout = state.call_timeout
    supervisor = Module.concat(state.group, TaskSupervisor)
    task = Task.Supervisor.async_nolink(supervisor, fn -> Remote.probe(pid, timeout) end)

    State.track_delivery(state, node, task.ref, :probe)
  end

  @doc "Applies the task's verdict, then flushes again if writes piled up behind the batch."
  @spec finish(
          State.t(),
          node(),
          State.cursor() | :probe,
          boolean(),
          Remote.verdict() | :ok | :error
        ) :: State.t()
  def finish(state, node, :probe, _truncated?, :ok) do
    state |> State.untrack_delivery(node) |> State.clear_suspect(node) |> Timers.flush_now()
  end

  def finish(state, node, :probe, _truncated?, :error) do
    state |> State.untrack_delivery(node) |> Timers.retry()
  end

  def finish(state, node, target, truncated?, verdict) do
    state
    |> State.untrack_delivery(node)
    |> apply_verdict(node, target, truncated?, verdict)
  end

  @doc "Drops a task that died without a verdict; the cursor stays, so a retry resends."
  @spec abort(State.t(), node()) :: State.t()
  def abort(state, node) do
    state |> State.untrack_delivery(node) |> Timers.retry()
  end

  # The batch is prepared here (reads own heap; big payloads stay refc-shared)
  # so the task carries only message wrappers, never the whole ring buffer. The
  # target is the cursor just past the batch: writes landing mid-flight, and
  # messages the batch limit left out, are not acked by it.
  defp start_task(state, pid, node, batch) do
    {target, truncated?} = target(state, node, batch)

    timeout = state.call_timeout
    supervisor = Module.concat(state.group, TaskSupervisor)

    task =
      Task.Supervisor.async_nolink(supervisor, fn -> Remote.deliver(pid, batch, timeout) end)

    report_batch(state, node, batch)
    State.track_delivery(state, node, task.ref, target, truncated?)
  end

  # Truncated: the limit cut the batch short, so the node is still behind.
  defp target(state, node, {:messages, messages}) do
    target = Map.fetch!(state.read_cursors, node) + length(messages)
    {target, target < state.write_cursor}
  end

  defp target(state, _node, {:expired, _missed}), do: {state.write_cursor, false}

  defp report_batch(state, node, {:messages, messages}),
    do: Telemetry.sync_batch(state.group, node, length(messages))

  defp report_batch(_state, _node, {:expired, _missed}), do: :ok

  defp apply_verdict(state, node, target, truncated?, :ok) do
    state |> State.ack(node, target) |> continue(node, truncated?)
  end

  defp apply_verdict(state, node, target, _truncated?, {:expired, missed}) do
    Telemetry.expired(state.group, node, missed)
    state |> State.ack(node, target) |> continue(node, false)
  end

  # A batch that timed out may have been too big for the node, not the node dead:
  # halve it so the next one can finish within call_timeout.
  defp apply_verdict(state, node, _target, _truncated?, {:failed, batch_size}) do
    Telemetry.sync_failure(state.group, node, batch_size)
    state |> State.shrink_batch_limit(node, batch_size) |> State.suspect(node) |> Timers.retry()
  end

  defp apply_verdict(state, node, _target, _truncated?, :expired_failed),
    do: state |> State.suspect(node) |> Timers.retry()

  # After a truncated batch the node is known to be behind by a full chunk, so
  # grow the limit and send the next chunk at once instead of waiting a window.
  defp continue(state, node, true),
    do: state |> State.grow_batch_limit(node) |> Timers.flush_now()

  defp continue(state, node, false),
    do: state |> State.reset_batch_limit(node) |> Timers.follow_up(node)
end
