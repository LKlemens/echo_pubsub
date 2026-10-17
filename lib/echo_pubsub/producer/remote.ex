defmodule EchoPubSub.Producer.Remote do
  @moduledoc """
  Calls the producer's tasks make to remote workers. Every failure (timeout,
  dead worker, `nodedown`, injected fault, non-`:ok` reply) becomes an error
  result, so a task never crashes.
  """

  require EchoPubSub.FaultInjection, as: FaultInjection

  @type registration :: {:register | :resume, node()}
  @type batch :: {:expired, pos_integer()} | {:messages, [term()]}
  @type verdict :: :ok | {:expired, pos_integer()} | :expired_failed | {:failed, pos_integer()}

  @doc "Sends `{:register, node}` or `{:resume, node}` to the worker."
  @spec register(pid(), registration(), timeout(), atom()) :: :ok | :error
  def register(pid, message, timeout, group), do: call(pid, message, timeout, group)

  @doc """
  Sends a batch, or `{:expired, node()}` for a node too far behind, and returns
  the verdict the producer folds into its state.
  """
  @spec deliver(pid(), batch(), timeout(), atom()) :: verdict()
  def deliver(pid, {:expired, missed}, timeout, group) do
    case call(pid, {:expired, node()}, timeout, group) do
      :ok -> {:expired, missed}
      :error -> :expired_failed
    end
  end

  def deliver(pid, {:messages, messages}, timeout, group) do
    case call(pid, messages, timeout, group) do
      :ok -> :ok
      :error -> {:failed, length(messages)}
    end
  end

  # Under an injected fault for `group`, outgoing calls short-circuit to :error:
  # the send side of a simulated partition.
  defp call(pid, message, timeout, group) do
    FaultInjection.if_ok group do
      do_call(pid, message, timeout)
    else
      :error
    end
  end

  defp do_call(pid, message, timeout) do
    case GenServer.call(pid, message, timeout) do
      :ok -> :ok
      _ -> :error
    end
  catch
    _, _ -> :error
  end
end
