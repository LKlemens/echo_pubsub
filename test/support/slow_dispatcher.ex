defmodule EchoPubSub.SlowDispatcher do
  @moduledoc """
  A `Phoenix.PubSub` dispatcher that models a costly local fan-out: it sleeps
  `:slow_dispatch_ms` per message, then delivers like `Phoenix.PubSub.dispatch/3`.

  The sleep applies only on nodes where `:echo_pubsub, :slow_dispatch_ms` is set,
  so a test can make one peer slow and leave the sender fast.
  """

  @spec dispatch([{pid(), term()}], pid() | :none, term()) :: :ok
  def dispatch(entries, from, message) do
    case Application.get_env(:echo_pubsub, :slow_dispatch_ms) do
      nil -> :ok
      ms -> Process.sleep(ms)
    end

    Phoenix.PubSub.dispatch(entries, from, message)
  end
end
