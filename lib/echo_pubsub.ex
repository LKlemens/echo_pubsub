defmodule EchoPubSub do
  @moduledoc """
  Phoenix PubSub adapter using :pg with at-least-once delivery.

  To start it, list it in your supervision tree as:

      {Phoenix.PubSub, name: MyApp.PubSub, adapter: EchoPubSub}

  You will also need to add `:echo_pubsub` to your deps:

      defp deps do
        [{:echo_pubsub, "~> 0.2.4"}]
      end

  ## Options

    * `:name` - The required name to register the PubSub processes, ie: `MyApp.PubSub`
    * `:pool_size` - The number of producers and workers to run on each node, allowing concurrent message delivery.
    * `:buffer_size` - The amount of messages to maintain in memory
    * `:batch_interval` - The interval in milliseconds to batch messages before sending (default: 200)
    * `:call_timeout` - The timeout in milliseconds for GenServer calls to remote nodes (default: 5000)
    * `:retry_interval` - The interval in milliseconds before retrying after a failed call (default: 200)
    * `:capacity_warning_threshold` - Buffer fill ratio (0.0-1.0) that triggers warning; above 1.0 never warns (default: 0.4)
    * `:capacity_warning_interval` - Minimum seconds between warnings (default: 60)

  ## Configuration

  Options can also be set via Application config per PubSub name:

      # config/config.exs
      config :echo_pubsub, MyApp.PubSub,
        pool_size: 2,
        buffer_size: 10_000,
        batch_interval: 100,
        call_timeout: 5000

  Options passed directly to the supervisor take precedence over config values.

  ### Non-blocking fan-out

  Each flush hands every remote node's batch to its own task and returns, so a
  flush costs the slowest round-trip instead of their sum and the producer keeps
  accepting writes while a slow or unreachable peer burns its `:call_timeout`.
  A node's read cursor advances only when its task reports an ack, and at most one
  batch per node is in flight, so batches still arrive in cursor order.

  ## Loading state at startup

  Load state only after the node is connected and subscribed:

    1. connect the node to the cluster
    2. subscribe to the topics
    3. load state from the source of truth (e.g. the database)

  Each peer registers this node on its own, shortly after the node joins, and
  only sends it the messages it flushes after that. A peer that had already
  flushed writes when it registers this node sends `{:cursor_expired, peer}`, so
  reload state on that message too: the load may have missed those writes.

  A notice that arrives before step 2 reaches no subscriber, which is harmless
  since step 3 follows. A node that subscribed early can get one notice per peer
  right after startup, so it is worth collapsing reloads that arrive together.

  A message broadcast before the load can still be delivered after it (batching
  delays delivery, failed sends are retried), so its change may already be in the
  loaded state: applying a message must tolerate seeing its change twice.

  ## Implementation

  The in memory buffer is a ring buffer, meaning that a constant number of messages are maintained and once
  the buffer is full, new messages overwrite the oldest message in the buffer.

  This means that if a node is unreachable long enough - a sustained network failure, or an actual
  disconnect - that its cursor points to a message that no longer exists, it will receive a special
  message over pubsub: `{:cursor_expired, node@host}`

  Applications are encouraged to handle and act on this message to get to a valid state, such as reloading all state from
  a source of truth like the db or another node. While this technically means that we don't guarentee every node will
  receive every message, we can guerentee that there are no gaps in messages. Recipt of message 3 guarentees you've
  received messages 1 and 2.

  """
  @behaviour Phoenix.PubSub.Adapter

  use Supervisor

  alias EchoPubSub.Options
  alias EchoPubSub.Producer
  alias EchoPubSub.Worker
  alias Phoenix.PubSub.Adapter

  ## Adapter callbacks

  @impl Adapter
  def node_name(_), do: node()

  @impl Adapter
  def broadcast(adapter_name, topic, message, dispatcher) do
    group = group(adapter_name)
    message = forward_to_local(topic, message, dispatcher)

    Producer.buffer_and_send(group, message)
  end

  @impl Adapter
  def direct_broadcast(adapter_name, node_name, topic, message, dispatcher) do
    GenServer.call(
      {Worker.name(group(adapter_name)), node_name},
      forward_to_local(topic, message, dispatcher)
    )
  end

  defp forward_to_local(topic, message, dispatcher) do
    {:forward_to_local, topic, message, dispatcher}
  end

  defp group(adapter_name) do
    groups = :persistent_term.get(adapter_name)
    elem(groups, :erlang.phash2(self(), tuple_size(groups)))
  end

  ## Supervisor callbacks

  @doc false
  def start_link(opts) do
    adapter_name = Keyword.fetch!(opts, :adapter_name)
    Supervisor.start_link(__MODULE__, opts, name: Module.concat(adapter_name, Supervisor))
  end

  @impl Supervisor
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    adapter_name = Keyword.fetch!(opts, :adapter_name)
    options = Options.resolve!(opts, Application.get_env(:echo_pubsub, name, []))

    groups = groups(adapter_name, options.pool_size)
    :persistent_term.put(adapter_name, List.to_tuple(groups))
    Enum.each(groups, &create_registrations_table/1)

    children = Enum.flat_map(groups, &group_children(&1, name, options))
    Supervisor.init(children, strategy: :one_for_one)
  end

  # Like PG2: `adapter_name`, `adapter_name2`... All nodes need the same pool_size.
  @spec groups(atom(), pos_integer()) :: [atom()]
  defp groups(adapter_name, pool_size) do
    [adapter_name | for(number <- 2..pool_size//1, do: :"#{adapter_name}#{number}")]
  end

  # Owned by this supervisor, so it outlives worker restarts.
  @spec create_registrations_table(atom()) :: atom()
  defp create_registrations_table(group) do
    :ets.new(Worker.registrations_table(group), [:set, :public, :named_table])
  end

  # Each side restarts under its own limit, so a crash-looping worker cannot take
  # the producer, and with it the buffer every peer replays from, down too.
  @spec group_children(atom(), atom(), Options.t()) :: [Supervisor.child_spec()]
  defp group_children(group, name, options) do
    producer_opts = [group: group] ++ Map.to_list(Map.delete(options, :pool_size))

    producer_side = [
      Supervisor.child_spec({Task.Supervisor, name: Module.concat(group, TaskSupervisor)},
        id: Module.concat(group, :TaskSupervisor)
      ),
      Supervisor.child_spec({Producer, producer_opts}, id: Module.concat(group, :Producer))
    ]

    worker_side = [
      Supervisor.child_spec({Worker, {name, group}}, id: Module.concat(group, :Worker))
    ]

    [
      supervisor_spec(Module.concat(group, ProducerSupervisor), producer_side, :rest_for_one),
      supervisor_spec(Module.concat(group, WorkerSupervisor), worker_side, :one_for_one)
    ]
  end

  @spec supervisor_spec(atom(), [Supervisor.child_spec()], Supervisor.strategy()) ::
          Supervisor.child_spec()
  defp supervisor_spec(name, children, strategy) do
    %{
      id: name,
      start: {Supervisor, :start_link, [children, [strategy: strategy, name: name]]},
      type: :supervisor
    }
  end
end
