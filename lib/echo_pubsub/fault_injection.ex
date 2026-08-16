defmodule EchoPubSub.FaultInjection do
  @moduledoc """
  Single source of truth for the compile-time fault-injection switch, shared by
  the producer (outgoing) and worker (incoming) to simulate a network partition.

  In production builds `ok?/0` is a hardcoded `true` that no runtime setting can
  flip, so injected faults can never activate. The switch is enabled under
  `:test`, or when a consuming app opts in for a demo:

      # config/config.exs
      config :echo_pubsub, :enable_fault_injection, true

  Once enabled, `ok?/0` honors the runtime `:fault_injection` flag, toggled with
  `Application.put_env(:echo_pubsub, :fault_injection, :error | :ok)`.
  """
  @enabled Application.compile_env(:echo_pubsub, :enable_fault_injection, Mix.env() == :test)

  @doc "Whether to deliver normally (true) or inject a fault (false)."
  @spec ok?() :: boolean()
  if @enabled do
    def ok?, do: Application.get_env(:echo_pubsub, :fault_injection, :ok) == :ok
  else
    def ok?, do: true
  end
end
