import Config

# Jepsen's db.clj sets these on every node; defaults suit a single local node.
if config_env() != :test do
  int = fn name, default -> name |> System.get_env(default) |> String.to_integer() end

  config :node_app,
    port: int.("PORT", "8080"),
    log_path: System.get_env("JEPSEN_LOG", "deliveries.log"),
    node_name: System.get_env("JEPSEN_NODE_NAME"),
    peers: System.get_env("JEPSEN_NODES", "") |> String.split(",", trim: true),
    cookie: System.get_env("JEPSEN_COOKIE", "jepsen-echo")

  config :echo_pubsub, NodeApp.PubSub,
    buffer_size: int.("ECHO_BUFFER_SIZE", "1000000"),
    batch_interval: int.("ECHO_BATCH_INTERVAL", "50"),
    call_timeout: int.("ECHO_CALL_TIMEOUT", "1000"),
    capacity_warning_threshold: 2.0
end
