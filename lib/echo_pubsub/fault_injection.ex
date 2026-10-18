defmodule EchoPubSub.FaultInjection do
  @moduledoc """
  Single source of truth for the compile-time fault-injection switch, shared by
  the producer (outgoing) and worker (incoming) to simulate a network partition.

  In production builds `ok?/0` is a hardcoded `true` that no runtime setting can
  flip, so injected faults can never activate. The switch is enabled under
  `:test` and `:bench` (the Fly benchmark), or when a consuming app opts in for a demo:

      # config/config.exs
      config :echo_pubsub, :enable_fault_injection, true

  Once enabled, `ok?/0` honors the runtime `:fault_injection` flag, toggled with
  `Application.put_env(:echo_pubsub, :fault_injection, :error | :ok)`.
  """
  @enabled Application.compile_env(
             :echo_pubsub,
             :enable_fault_injection,
             Mix.env() in [:test, :bench]
           )

  @doc "Whether to deliver normally (true) or inject a fault (false)."
  @spec ok?() :: boolean()
  if @enabled do
    def ok?, do: Application.get_env(:echo_pubsub, :fault_injection, :ok) == :ok
  else
    def ok?, do: true
  end

  @doc """
  Runs the `do` block, or the `else` block while a fault is injected.

  When fault injection is off, only the `do` block is compiled, so production code
  has no fault branch at all.

      FaultInjection.if_ok do
        deliver()
      else
        :error
      end
  """
  # Two definitions, picked at compile time: a runtime `if @enabled` inside one
  # macro would leave Dialyzer an unreachable branch in whichever build it checks.
  if @enabled do
    defmacro if_ok(do: ok_block, else: fault_block) do
      quote do
        if EchoPubSub.FaultInjection.ok?(), do: unquote(ok_block), else: unquote(fault_block)
      end
    end
  else
    defmacro if_ok(do: ok_block, else: _fault_block), do: ok_block
  end
end
