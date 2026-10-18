defmodule EchoPubSub.Producer.Remote do
  @moduledoc """
  Calls the producer's tasks make to remote workers. Every failure (timeout,
  dead worker, `nodedown`, injected fault, non-`:ok` reply) becomes an error
  result, so a task never crashes.
  """

  require EchoPubSub.FaultInjection, as: FaultInjection

  @type registration :: {:register, node(), boolean()} | {:resume, node()}
  @type batch :: {:expired, pos_integer()} | {:messages, [term()]}
  @type verdict :: :ok | {:expired, pos_integer()} | :expired_failed | {:failed, pos_integer()}

  @doc "Sends `{:register, node, missed_writes?}` or `{:resume, node}` to the worker."
  @spec register(pid(), registration(), timeout()) :: :ok | :error
  def register(pid, message, timeout), do: call(pid, message, timeout)

  @doc """
  Pings the worker to learn whether it answers again. A worker older than
  `:ping` replies `{:error, :bad_message}`, which still proves it is alive.
  """
  @spec probe(pid(), timeout()) :: :ok | :error
  def probe(pid, timeout) do
    FaultInjection.if_ok do
      do_probe(pid, timeout)
    else
      :error
    end
  end

  defp do_probe(pid, timeout) do
    case GenServer.call(pid, :ping, timeout) do
      :ok -> :ok
      {:error, :bad_message} -> :ok
      _ -> :error
    end
  catch
    _, _ -> :error
  end

  @doc """
  Sends a batch, or `{:expired, node()}` for a node too far behind, and returns
  the verdict the producer folds into its state.
  """
  @spec deliver(pid(), batch(), timeout()) :: verdict()
  def deliver(pid, {:expired, missed}, timeout) do
    case call(pid, {:expired, node()}, timeout) do
      :ok -> {:expired, missed}
      :error -> :expired_failed
    end
  end

  def deliver(pid, {:messages, messages}, timeout) do
    case call(pid, messages, timeout) do
      :ok -> :ok
      :error -> {:failed, length(messages)}
    end
  end

  # Under an injected fault, outgoing calls short-circuit to :error: the send
  # side of a simulated partition.
  defp call(pid, message, timeout) do
    FaultInjection.if_ok do
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
