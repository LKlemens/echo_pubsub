defmodule EchoPubSub.Bench.App do
  @moduledoc """
  OTP application for the Fly.io benchmark cluster (`:bench` release only). Clusters
  the machines over Fly's private network, starts the EchoPubSub-backed PubSub, and
  a subscribed collector. Trigger a run from one node with `EchoPubSub.Bench.Runner`.

  `POOL_SIZE`/`BUFFER_SIZE`/`BATCH_INTERVAL` env are the *boot* defaults; the PubSub
  runs under a restartable child so `reconfigure/1` can swap them at runtime (used by
  `Runner.run/1` to sweep without a redeploy).
  """
  use Application
  require Logger

  alias EchoPubSub.BenchCollector

  @topic "bench"
  @pubsub PubSubTest
  @pubsub_child :bench_pubsub
  @opts_key :bench_opts

  @impl Application
  def start(_type, _args) do
    opts = current_opts()
    Logger.info("[bench] starting app on #{node()} with #{inspect(opts)}")

    children = [
      {Cluster.Supervisor, [topology(), [name: EchoPubSub.Bench.ClusterSupervisor]]},
      pubsub_child(opts)
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: EchoPubSub.Bench.Supervisor)
  end

  @doc """
  The PubSub opts currently in effect on this node: the last `reconfigure/1` values,
  or the boot env defaults (`fly.toml`) if never reconfigured. Persisted in the
  application env so it survives across runs and reflects what the live PubSub runs.
  """
  @spec current_opts() :: keyword()
  def current_opts do
    Application.get_env(:echo_pubsub, @opts_key, boot_opts())
  end

  @doc """
  Restart the PubSub (and its collector) with new `pool_size`/`buffer_size`/
  `batch_interval` opts, merged over the current ones and persisted. Call on every
  node before a run to sweep a knob without redeploying.
  """
  @spec reconfigure(keyword()) :: :ok
  def reconfigure(opts) do
    sup = EchoPubSub.Bench.Supervisor

    merged =
      Keyword.merge(
        current_opts(),
        Keyword.take(opts, [:pool_size, :buffer_size, :batch_interval])
      )

    Logger.info("[bench] reconfiguring PubSub on #{node()} -> #{inspect(merged)}")

    Application.put_env(:echo_pubsub, @opts_key, merged)
    :ok = Supervisor.terminate_child(sup, @pubsub_child)
    :ok = Supervisor.delete_child(sup, @pubsub_child)
    {:ok, _} = Supervisor.start_child(sup, pubsub_child(merged))

    Logger.info("[bench] PubSub restarted on #{node()}")
    :ok
  end

  # PubSub + collector as one restartable subtree. rest_for_one so the collector
  # (which subscribes in its init) is restarted after - and re-subscribes to - any
  # PubSub restart.
  defp pubsub_child(opts) do
    %{
      id: @pubsub_child,
      type: :supervisor,
      start: {__MODULE__, :start_pubsub, [opts]}
    }
  end

  @doc false
  def start_pubsub(opts) do
    Logger.info("[bench] starting PubSub on #{node()} with #{inspect(opts)}")

    children = [
      {Phoenix.PubSub,
       [
         name: @pubsub,
         adapter: EchoPubSub,
         pool_size: opts[:pool_size],
         buffer_size: opts[:buffer_size],
         batch_interval: opts[:batch_interval]
       ]},
      {BenchCollector, @topic}
    ]

    Supervisor.start_link(children,
      strategy: :rest_for_one,
      name: EchoPubSub.Bench.PubSubSupervisor
    )
  end

  defp boot_opts do
    [
      pool_size: env_int("POOL_SIZE", 1),
      buffer_size: env_int("BUFFER_SIZE", 200_000),
      batch_interval: env_int("BATCH_INTERVAL", 100)
    ]
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
