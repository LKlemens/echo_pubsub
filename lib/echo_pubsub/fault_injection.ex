defmodule EchoPubSub.FaultInjection do
  @moduledoc """
  Single source of truth for the compile-time fault-injection switch, shared by
  the producer (outgoing) and worker (incoming) to simulate a network partition.

  In production builds the checks are a hardcoded `true` that no runtime setting
  can flip, so injected faults can never activate. The switch is enabled under
  `:test`, or when a consuming app opts in for a demo:

      # config/config.exs
      config :echo_pubsub, :enable_fault_injection, true

  Once enabled, `ok?/0` honors the global runtime `:fault_injection` flag,
  toggled with `Application.put_env(:echo_pubsub, :fault_injection, :error | :ok)`.

  `ok?/1` scopes a fault to a single pubsub group, so callers running many
  instances side by side can partition one without touching the others. The
  per-group flag lives in `:persistent_term` (fast reads on the delivery hot
  path, and independent keys so lanes never race each other), set via `put/2`:

      EchoPubSub.FaultInjection.put(MyApp.Bus, :error)

  A per-group flag wins when set; otherwise the global flag applies.
  """
  @enabled Application.compile_env(:echo_pubsub, :enable_fault_injection, Mix.env() == :test)

  @spec ok?() :: boolean()
  @spec ok?(atom()) :: boolean()
  @spec put(atom(), :ok | :error) :: :ok
  if @enabled do
    @doc "Whether to deliver normally (true) or inject a fault (false), globally."
    def ok?, do: Application.get_env(:echo_pubsub, :fault_injection, :ok) == :ok

    @doc "Whether to deliver normally for `group`; a per-group flag overrides the global one."
    def ok?(group), do: :persistent_term.get({__MODULE__, group}, global()) == :ok

    @doc "Sets the per-group fault flag."
    def put(group, status) when status in [:ok, :error] do
      :persistent_term.put({__MODULE__, group}, status)
    end

    defp global, do: Application.get_env(:echo_pubsub, :fault_injection, :ok)
  else
    def ok?, do: true
    def ok?(_group), do: true
    def put(_group, _status), do: :ok
  end
end
