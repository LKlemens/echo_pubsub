defmodule NodeApp.Application do
  @moduledoc """
  One Jepsen node. Children start in dependency order, so that by the time a peer
  can reach this node the subscriber is listening, and by the time Jepsen can
  broadcast the node is in the cluster.
  """
  use Application

  @impl Application
  def start(_type, _args) do
    :persistent_term.put(:node_app_boot_id, boot_id())

    children = [
      {Phoenix.PubSub, name: NodeApp.PubSub, adapter: EchoPubSub},
      {NodeApp.Subscriber, Application.fetch_env!(:node_app, :log_path)},
      {NodeApp.Cluster,
       node_name: Application.get_env(:node_app, :node_name),
       peers: Application.get_env(:node_app, :peers, []),
       cookie: Application.get_env(:node_app, :cookie, "jepsen-echo")},
      {Bandit, plug: NodeApp.Router, port: Application.fetch_env!(:node_app, :port)}
    ]

    Supervisor.start_link(children, strategy: :rest_for_one, name: NodeApp.Supervisor)
  end

  @doc """
  Identifies this run of the VM. A restarted node starts a new producer with an
  empty buffer, so the checker treats each boot as its own message stream.

      iex> is_binary(NodeApp.Application.boot())
      true
  """
  @spec boot() :: String.t()
  def boot, do: :persistent_term.get(:node_app_boot_id)

  defp boot_id, do: 8 |> :crypto.strong_rand_bytes() |> Base.encode16(case: :lower)
end
