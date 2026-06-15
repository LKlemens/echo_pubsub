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

  def spawn_node(name, nodes, opts \\ []) do
    {:ok, pid, node} = :peer.start_link(%{name: ~c"#{name}", connection: :standard_io})

    :peer.call(pid, :code, :add_paths, [:code.get_path()])
    connect_to_cluster(pid, nodes)
    transfer_config(pid)
    start_apps(pid, opts)

    %{node: node, pid: pid}
  end

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

  # Dev/test build tooling that has no business running on a test peer node.
  @skip_apps [:dialyxir, :credo, :ex_doc, :erlex]

  defp start_apps(pid, opts) do
    :peer.call(pid, Application, :ensure_all_started, [:mix])
    :peer.call(pid, Mix, :env, [Mix.env()])

    for {app_name, _, _} <- Application.loaded_applications(), app_name not in @skip_apps do
      :peer.call(pid, Application, :ensure_all_started, [app_name])
    end

    batch_interval = Keyword.get(opts, :batch_interval, 0)
    capacity_warning_threshold = Keyword.get(opts, :capacity_warning_threshold, 0.4)
    capacity_warning_interval = Keyword.get(opts, :capacity_warning_interval, 60)

    remote_run %{pid: pid},
      batch_interval: batch_interval,
      capacity_warning_threshold: capacity_warning_threshold,
      capacity_warning_interval: capacity_warning_interval do
      parent = self()

      spawn(fn ->
        children = [
          {Phoenix.PubSub,
           name: PubSubTest,
           adapter: EchoPubSub,
           pool_size: 1,
           buffer_size: 10,
           batch_interval: batch_interval,
           capacity_warning_threshold: capacity_warning_threshold,
           capacity_warning_interval: capacity_warning_interval},
          {EchoPubSub.TestSubscriber, name: EchoPubSub.TestSubscriber}
        ]

        {:ok, supervisor_pid} =
          Supervisor.start_link(children,
            strategy: :one_for_one,
            name: EchoPubSub.TestSupervisor
          )

        send(parent, {:started, supervisor_pid})

        Process.sleep(:infinity)
      end)

      receive do
        {:started, pid} -> {:ok, pid}
      end
    end
  end
end
