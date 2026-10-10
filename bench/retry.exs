# One failing node, growing retry batch. Spawns three :peer nodes (A publishes,
# B is healthy, C fails) and runs EchoPubSub.RetryBench on A.
#
#   MIX_ENV=test mix run bench/retry.exs   # needs :test for the cluster helpers
#
# Env vars (defaults in parens):
#   FAIL_MODE      reject | hung                    (reject)
#   STEPS          comma list of backlog sizes      (6000,7000,8000,9000,10000)
#   PAYLOAD_SIZES  comma list of message bytes      (200,700)
#   BATCH_INTERVAL producer batch interval (ms)     (100, same as Fly)

alias EchoPubSub.Cluster

env_list = fn name, default ->
  System.get_env(name, default)
  |> String.split(",", trim: true)
  |> Enum.map(&String.to_integer(String.trim(&1)))
end

mode =
  case System.get_env("FAIL_MODE", "reject") do
    "reject" -> :reject
    "hung" -> :hung
  end

steps = env_list.("STEPS", "6000,7000,8000,9000,10000")
payloads = env_list.("PAYLOAD_SIZES", "200,700")
batch_interval = System.get_env("BATCH_INTERVAL", "100") |> String.to_integer()

run_id = System.unique_integer([:positive])

# Buffer well above the largest step, so C's backlog never expires.
[a, b, c] =
  Cluster.spawn_nodes(Enum.map(~w(a b c), &"retry#{run_id}_#{&1}"),
    buffer_size: 4 * Enum.max(steps),
    batch_interval: batch_interval,
    capacity_warning_threshold: 2.0
  )

Process.sleep(300)

:peer.call(
  a.pid,
  EchoPubSub.RetryBench,
  :run,
  [[healthy: b.node, failing: c.node, mode: mode, steps: steps, payloads: payloads]],
  :infinity
)

Enum.each([a, b, c], &:peer.stop(&1.pid))
