defmodule EchoPubSub.Producer.Delivery do
  @moduledoc """
  Sends a registered node what it still needs, in a task. The node's cursor moves
  to the batch's target only when its worker acks, so a failed batch is resent.
  """

  alias EchoPubSub.Producer.State
  alias EchoPubSub.Producer.Remote
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

  @doc "Applies the task's verdict, then flushes again if writes piled up behind the batch."
  @spec finish(State.t(), node(), State.cursor(), Remote.verdict()) :: State.t()
  def finish(state, node, target, verdict) do
    state
    |> State.untrack_delivery(node)
    |> apply_verdict(node, target, verdict)
    |> Timers.follow_up(node)
  end

  @doc "Drops a task that died without a verdict; the cursor stays, so a retry resends."
  @spec abort(State.t(), node()) :: State.t()
  def abort(state, node) do
    state |> State.untrack_delivery(node) |> Timers.retry()
  end

  # The batch is prepared here (reads own heap; big payloads stay refc-shared)
  # so the task carries only message wrappers, never the whole ring buffer. The
  # target is the write cursor now: writes landing mid-flight are not acked by it.
  defp start_task(state, pid, node, batch) do
    timeout = state.call_timeout
    supervisor = Module.concat(state.group, TaskSupervisor)

    task =
      Task.Supervisor.async_nolink(supervisor, fn -> Remote.deliver(pid, batch, timeout) end)

    State.track_delivery(state, node, task.ref, state.write_cursor)
  end

  defp apply_verdict(state, node, target, :ok), do: State.ack(state, node, target)

  defp apply_verdict(state, node, target, {:expired, missed}) do
    Telemetry.expired(state.group, node, missed)
    State.ack(state, node, target)
  end

  defp apply_verdict(state, node, _target, {:failed, batch_size}) do
    Telemetry.sync_failure(state.group, node, batch_size)
    Timers.retry(state)
  end

  defp apply_verdict(state, _node, _target, :expired_failed), do: Timers.retry(state)
end
