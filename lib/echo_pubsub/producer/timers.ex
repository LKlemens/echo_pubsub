defmodule EchoPubSub.Producer.Timers do
  @moduledoc "Schedules the producer's `:flush_all`: batch flushes, retries and follow-ups. One timer at a time."

  alias EchoPubSub.Producer.State
  alias EchoPubSub.Producer.Telemetry

  @doc "Flushes after `batch_interval` (at once when `0`), unless a flush is already pending."
  @spec flush_soon(State.t()) :: State.t()
  def flush_soon(%State{batch_interval: 0} = state) do
    send(self(), :flush_all)
    state
  end

  def flush_soon(state) do
    if is_nil(state.flush_timer) do
      State.set_flush_timer(state, Process.send_after(self(), :flush_all, state.batch_interval))
    else
      state
    end
  end

  @doc "Flushes again after `retry_interval` ms following a failure, unless a flush is already pending."
  @spec retry(State.t()) :: State.t()
  def retry(state) do
    if is_nil(state.flush_timer) do
      Telemetry.retry_scheduled(state.group)
      State.set_flush_timer(state, Process.send_after(self(), :flush_all, state.retry_interval))
    else
      state
    end
  end

  @doc """
  Flushes soon if `node` is still behind: writes that landed while it was
  registering or mid-delivery were skipped by the flush that ran then.
  """
  @spec follow_up(State.t(), node()) :: State.t()
  def follow_up(state, node) do
    if State.behind?(state, node), do: flush_soon(state), else: state
  end
end
