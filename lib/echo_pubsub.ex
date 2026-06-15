defmodule EchoPubSub do
  @moduledoc """
  Phoenix PubSub adapter using :pg with at-least-once delivery.

  To start it, list it in your supervision tree as:

      {Phoenix.PubSub, name: MyApp.PubSub, adapter: EchoPubSub}

  You will also need to add `:echo_pubsub` to your deps:

      defp deps do
        [{:echo_pubsub, "~> 0.1.0"}]
      end

  ## Options

    * `:name` - The required name to register the PubSub processes, ie: `MyApp.PubSub`
    * `:pool_size` - The number of producers and workers to run on each node, allowing concurrent message delivery.
    * `:buffer_size` - The amount of messages to maintain in memory
    * `:batch_interval` - The interval in milliseconds to batch messages before sending (default: 200)
    * `:call_timeout` - The timeout in milliseconds for GenServer calls to remote nodes (default: 5000)
    * `:capacity_warning_threshold` - Buffer fill ratio (0.0-1.0) that triggers warning (default: 0.4)
    * `:capacity_warning_interval` - Minimum seconds between warnings (default: 60)

  ## Configuration

  Options can also be set via Application config per PubSub name:

      # config/config.exs
      config :echo_pubsub, MyApp.PubSub,
        pool_size: 2,
        buffer_size: 50_000,
        batch_interval: 100,
        call_timeout: 5000

  Options passed directly to the supervisor take precedence over config values.

  ## Implementation

  The in memory buffer is a ring buffer, meaning that a constant number of messages are maintained and once
  the buffer is full, new messages overwrite the oldest message in the buffer.

  This means that if a node in the cluster is disconnected long enough that when it reconnects, its cursor
  points to a message that no longer exists, it will receive a special message over pubsub: `{:cursor_expired, node@host}`

  Applications are encouraged to handle and act on this message to get to a valid state, such as reloading all state from
  a source of truth like the db or another node. While this technically means that we don't guarentee every node will
  receive every message, we can guerentee that there are no gaps in messages. Recipt of message 3 guarentees you've
  received messages 1 and 2.

  """
  @behaviour Phoenix.PubSub.Adapter

  use Supervisor

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
      {Module.concat(group(adapter_name), :Worker), node_name},
      {:forward_to_local, topic, message, dispatcher}
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

    # Read from Application config, with opts taking precedence
    config = Application.get_env(:echo_pubsub, name, [])
    pool_size = Keyword.get(opts, :pool_size) || Keyword.get(config, :pool_size, 1)
    buffer_size = Keyword.get(opts, :buffer_size) || Keyword.get(config, :buffer_size, 10_000)

    batch_interval =
      Keyword.get(opts, :batch_interval) || Keyword.get(config, :batch_interval, 200)

    call_timeout =
      Keyword.get(opts, :call_timeout) || Keyword.get(config, :call_timeout, 5000)

    capacity_warning_threshold =
      Keyword.get(opts, :capacity_warning_threshold) ||
        Keyword.get(config, :capacity_warning_threshold, 0.4)

    capacity_warning_interval =
      Keyword.get(opts, :capacity_warning_interval) ||
        Keyword.get(config, :capacity_warning_interval, 60)

    [_ | groups] =
      for number <- 1..pool_size do
        :"#{adapter_name}#{number}"
      end

    # Use `adapter_name` for the first in the pool for backwards compatability
    # with v2.0 when the pool_size is 1.
    groups = [adapter_name | groups]

    :persistent_term.put(adapter_name, List.to_tuple(groups))

    children =
      Enum.flat_map(groups, fn group ->
        producer_id = Module.concat(group, :Producer)

        [
          Supervisor.child_spec(
            {Producer,
             {buffer_size, batch_interval, call_timeout, capacity_warning_threshold,
              capacity_warning_interval, group}},
            id: producer_id
          ),
          Supervisor.child_spec({Worker, {name, group}}, id: Module.concat(group, :Worker))
        ]
      end)

    Supervisor.init(children, strategy: :one_for_one)
  end
end
