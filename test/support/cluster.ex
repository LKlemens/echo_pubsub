defmodule EchoPubSub.Cluster do
  @moduledoc "Test helpers for spawning peer nodes and asserting on messages across a cluster."

  def spawn_nodes(node_names, opts \\ []) do
    node_names
    |> Enum.reduce([], fn name, nodes -> [spawn_node(name, nodes, opts) | nodes] end)
    |> Enum.reverse()
  end

  def apply(node, module, fun, args) do
    :peer.call(node.pid, module, fun, args)
  end

  defmacro remote_run(node, args \\ [], do: block) do
    quote do
      pid = unquote(node).pid
      block = unquote(Macro.escape(block))
      {result, _binding} = :peer.call(pid, Code, :eval_quoted, [block, unquote(args)], 1000)
      result
    end
  end

  defmacro assert_peer_receive(peer, pattern, timeout \\ 5_000) do
    quote do
      start_time = System.monotonic_time(:millisecond)
      match_fn = fn message -> match?(unquote(pattern), message) end

      EchoPubSub.Cluster.remote_message_check(
        unquote(peer),
        match_fn,
        :assert,
        start_time,
        unquote(timeout)
      )
    end
  end

  defmacro refute_peer_receive(peer, pattern, timeout \\ 300) do
    quote do
      start_time = System.monotonic_time(:millisecond)
      match_fn = fn message -> match?(unquote(pattern), message) end

      EchoPubSub.Cluster.remote_message_check(
        unquote(peer),
        match_fn,
        :refute,
        start_time,
        unquote(timeout)
      )
    end
  end

  def remote_message_check(peer, match_fn, type, start_time, timeout, messages \\ []) do
    now = System.monotonic_time(:millisecond)

    message =
      remote_run peer do
        EchoPubSub.TestSubscriber.get_message()
      end

    messages = if is_nil(message), do: messages, else: messages ++ [message]

    case {match_fn.(message), type} do
      {true, :assert} ->
        true

      {true, :refute} ->
        ExUnit.Assertions.flunk("Found unexpected message #{inspect(message)}")

      {false, _type} when now - start_time < timeout ->
        Process.sleep(10)
        remote_message_check(peer, match_fn, type, start_time, timeout, messages)

      {false, :assert} ->
        ExUnit.Assertions.flunk(
          "Failed to find matching message in timeout, messages in mailbox #{inspect(messages)}"
        )

      {false, :refute} ->
        true
    end
  end

  def assert_wait_for(fun), do: assert_wait_for(1000, fun)

  def assert_wait_for(timeout, fun) when timeout <= 0, do: fun.()

  def assert_wait_for(timeout, fun) do
    fun.()
  rescue
    ExUnit.AssertionError ->
      Process.sleep(10)
      assert_wait_for(max(0, timeout - 10), fun)
  end

  @doc "Stops the PubSub server and TestSubscriber on the calling node, newest first."
  def stop_children do
    for {id, _pid, _type, _modules} <- Supervisor.which_children(EchoPubSub.TestSupervisor) do
      :ok = Supervisor.terminate_child(EchoPubSub.TestSupervisor, id)
    end

    :ok
  end

  @doc "Starts the children stopped by stop_children/0 again, oldest first."
  def start_children do
    for {id, _pid, _type, _modules} <-
          Enum.reverse(Supervisor.which_children(EchoPubSub.TestSupervisor)) do
      {:ok, _pid} = Supervisor.restart_child(EchoPubSub.TestSupervisor, id)
    end

    :ok
  end

  def spawn_node(name, nodes, opts \\ []) do
    # Async test modules boot peers at the same time, so names must never repeat.
    name = "#{name}_#{System.unique_integer([:positive])}"
    # Tests cut single links on purpose; without this, 'global' would cut more
    # links to stop the cluster splitting into overlapping partitions.
    args = [~c"-kernel", ~c"prevent_overlapping_partitions", ~c"false"]
    {pid, node} = start_peer(%{name: ~c"#{name}", connection: :standard_io, args: args})

    :peer.call(pid, :code, :add_paths, [:code.get_path()])
    connect_to_cluster(pid, nodes)
    transfer_config(pid)
    start_apps(pid, opts)

    %{node: node, pid: pid}
  end

  # With coverage on (see test_helper.exs), the peer runs the cover-compiled
  # modules and reports back before it stops. A linked peer would die with the
  # test process before on_exit could flush its data, so it is stopped there.
  defp start_peer(options) do
    if cover_running?() do
      cookie = Atom.to_charlist(Node.get_cookie())
      {:ok, pid, node} = :peer.start(%{options | args: options.args ++ [~c"-setcookie", cookie]})
      true = Node.connect(node)
      {:ok, _node} = :cover.start(node)

      ExUnit.Callbacks.on_exit(fn ->
        :cover.flush(node)
        :peer.stop(pid)
      end)

      {pid, node}
    else
      {:ok, pid, node} = :peer.start_link(options)
      {pid, node}
    end
  end

  defp cover_running?, do: Node.alive?() and Process.whereis(:cover_server) != nil

  defp connect_to_cluster(pid, [%{node: last_node} | _]) do
    :peer.call(pid, :net_kernel, :connect_node, [last_node])
  end

  defp connect_to_cluster(_pid, []), do: :ok

  defp transfer_config(pid) do
    # transfer app configuration
    Application.loaded_applications()
    |> Enum.map(fn {app_name, _, _} -> app_name end)
    |> Enum.map(fn app_name -> {app_name, Application.get_all_env(app_name)} end)
    |> Enum.each(fn {app_name, env} ->
      Enum.each(env, fn {key, val} ->
        :ok = :peer.call(pid, Application, :put_env, [app_name, key, val, [persistent: true]])
      end)
    end)
  end

  # Only what a peer runs: the library itself, ExUnit for CaptureLog, and Mix for
  # Mix.env. Starting every loaded app (hex, ssl, credo...) slows each boot.
  @peer_apps [:mix, :ex_unit, :echo_pubsub]

  defp start_apps(pid, opts) do
    for app_name <- @peer_apps do
      {:ok, _} = :peer.call(pid, Application, :ensure_all_started, [app_name])
    end

    :peer.call(pid, Mix, :env, [Mix.env()])

    # A cold peer loads every module from disk here, which can outlast
    # remote_run's 1 s timeout on a busy CI runner.
    {:ok, _supervisor} = :peer.call(pid, __MODULE__, :start_supervisor, [opts], 10_000)
  end

  @doc """
  Starts PubSubTest and the TestSubscriber under `EchoPubSub.TestSupervisor` on
  the calling node, owned by a process that never exits. Runs on the peer.
  """
  def start_supervisor(opts) do
    pubsub_opts = [
      name: PubSubTest,
      adapter: EchoPubSub,
      pool_size: Keyword.get(opts, :pool_size, 1),
      buffer_size: Keyword.get(opts, :buffer_size, 10),
      batch_interval: Keyword.get(opts, :batch_interval, 0),
      call_timeout: Keyword.get(opts, :call_timeout, 5000),
      retry_interval: Keyword.get(opts, :retry_interval, 50),
      capacity_warning_threshold: Keyword.get(opts, :capacity_warning_threshold, 0.4),
      capacity_warning_interval: Keyword.get(opts, :capacity_warning_interval, 60)
    ]

    children = [
      {Phoenix.PubSub, pubsub_opts},
      {EchoPubSub.TestSubscriber, name: EchoPubSub.TestSubscriber}
    ]

    parent = self()

    spawn(fn ->
      {:ok, supervisor_pid} =
        Supervisor.start_link(children, strategy: :one_for_one, name: EchoPubSub.TestSupervisor)

      send(parent, {:started, supervisor_pid})
      Process.sleep(:infinity)
    end)

    receive do
      {:started, pid} -> {:ok, pid}
    end
  end
end
