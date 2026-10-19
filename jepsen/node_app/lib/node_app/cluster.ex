defmodule NodeApp.Cluster do
  @moduledoc """
  Starts distribution, then keeps reconnecting to the peers.

  The release boots with distribution off and this process starts it, so no peer
  can deliver to this node before `NodeApp.Subscriber` is listening; a delivery
  that arrived first would be acked and silently dropped. Erlang does not
  reconnect on its own after a partition heals, hence the retry loop. Each
  connect runs in its own task: connecting to a partitioned peer blocks for
  seconds, and must not hold up startup or the other peers.
  """
  use GenServer

  @reconnect_interval 1_000

  @type opt :: {:node_name, String.t() | nil} | {:peers, [String.t()]} | {:cookie, String.t()}

  @doc "Starts the cluster process. Without `:node_name` it stays local."
  @spec start_link([opt()]) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl GenServer
  def init(opts) do
    case Keyword.get(opts, :node_name) do
      nil ->
        :ignore

      name ->
        # Already up when this process restarts after a crash of the subscriber.
        unless Node.alive?(), do: {:ok, _} = Node.start(String.to_atom(name), :shortnames)
        Node.set_cookie(opts |> Keyword.fetch!(:cookie) |> String.to_atom())
        peers = opts |> Keyword.get(:peers, []) |> Enum.map(&String.to_atom/1)
        send(self(), :reconnect)
        {:ok, peers}
    end
  end

  @impl GenServer
  def handle_info(:reconnect, peers), do: {:noreply, connect(peers)}

  defp connect(peers) do
    for peer <- peers, peer != node(), peer not in Node.list() do
      Task.start(fn -> Node.connect(peer) end)
    end

    Process.send_after(self(), :reconnect, @reconnect_interval)
    peers
  end
end
