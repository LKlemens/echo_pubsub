defmodule EchoPubSub.Bench.App do
  @moduledoc """
  OTP application for the Fly.io benchmark cluster (`:bench` release only). Clusters
  the machines over Fly's private network, starts the EchoPubSub-backed PubSub, and
  a subscribed collector. Trigger a run from one node with `EchoPubSub.Bench.Runner`.
  """
  use Application

  @topic "bench"

  @impl Application
  def start(_type, _args) do
    children = [
      {Cluster.Supervisor, [topology(), [name: EchoPubSub.Bench.ClusterSupervisor]]},
      {Phoenix.PubSub,
       name: PubSubTest,
       adapter: EchoPubSub,
       pool_size: env_int("POOL_SIZE", 1),
       buffer_size: env_int("BUFFER_SIZE", 200_000),
       batch_interval: env_int("BATCH_INTERVAL", 100)},
      {EchoPubSub.BenchCollector, @topic}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: EchoPubSub.Bench.Supervisor)
  end

  defp topology do
    app = System.get_env("FLY_APP_NAME") || "echo-pubsub-bench"

    [
      fly: [
        strategy: Cluster.Strategy.DNSPoll,
        config: [
          polling_interval: 5_000,
          query: "#{app}.internal",
          node_basename: app
        ]
      ]
    ]
  end

  defp env_int(key, default) do
    (System.get_env(key) || Integer.to_string(default)) |> String.to_integer()
  end
end
