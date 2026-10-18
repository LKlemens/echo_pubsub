defmodule EchoPubSub.ClusterCase do
  @moduledoc """
  Case template for tests that run against a cluster of peer nodes.

  Each module sets up its own peers: either it shares one pair across its tests
  with `setup_all :start_shared_peers` and `setup :reset_peers`, or its tests
  spawn their own.
  """
  use ExUnit.CaseTemplate

  import EchoPubSub.Cluster

  using do
    quote do
      import EchoPubSub.Cluster
      import EchoPubSub.ClusterCase
    end
  end

  @doc """
  Starts two connected peers, `peer1` and `peer2`, shared by every test in the
  module. Use as `setup_all :start_shared_peers`, together with
  `setup :reset_peers`.
  """
  @spec start_shared_peers(map()) :: %{peer1: map(), peer2: map()}
  def start_shared_peers(_context) do
    # Expiry/overflow tests intentionally fill the buffer; capacity warnings are
    # exercised separately, so disable them here to keep test output clean.
    [peer1, peer2] = spawn_nodes(["node1", "node2"], capacity_warning_threshold: 2.0)
    subscribe_and_wait(peer1, peer2)
    %{peer1: peer1, peer2: peer2}
  end

  @doc """
  Puts the shared peers back in their default state after each test. Use as
  `setup :reset_peers`.

  Restarting the PubSub tree is the one reset that clears everything a test can
  leave behind: buffers, cursors, registrations, killed or suspended processes
  and subscriber messages.
  """
  @spec reset_peers(map()) :: :ok
  def reset_peers(%{peer1: peer1, peer2: peer2}) do
    ExUnit.Callbacks.on_exit(fn ->
      restart_cluster([peer1, peer2])
      subscribe_and_wait(peer1, peer2)
    end)
  end

  @doc """
  Clears faults, reconnects every peer and restarts each PubSub tree, which also
  drops the subscribers' messages and subscriptions.
  """
  @spec restart_cluster([map()]) :: :ok
  def restart_cluster(peers) do
    for peer <- peers do
      remote_run peer, nodes: Enum.map(peers, & &1.node) do
        Application.put_env(:echo_pubsub, :fault_injection, :ok)
        Enum.each(nodes, &Node.connect/1)
      end
    end

    # Every tree stops before any starts again: a worker that survived would
    # remember the other producer and report a stray cursor_expired.
    for peer <- peers, do: remote_run(peer, do: EchoPubSub.Cluster.stop_children())
    for peer <- peers, do: remote_run(peer, do: EchoPubSub.Cluster.start_children())

    :ok
  end

  defp subscribe_and_wait(peer1, peer2) do
    remote_run peer2 do
      EchoPubSub.TestSubscriber.subscribe(PubSubTest, "topic")
    end

    assert_registered(peer2, peer1)
    assert_registered(peer1, peer2)
  end

  @doc """
  Waits until `producer_peer`'s producer has a read cursor for `peer`: it has
  handled `peer`'s registration reply and will include it in flushes.
  """
  @spec assert_registered(map(), map()) :: true
  def assert_registered(peer, producer_peer) do
    assert_wait_for(5_000, fn ->
      read_cursors =
        remote_run producer_peer do
          :sys.get_state(PubSubTest.Adapter.Producer).read_cursors
        end

      ExUnit.Assertions.assert(Map.has_key?(read_cursors, peer.node))
    end)
  end

  @doc """
  Every message `peer`'s subscriber gets up to and including `last`, in order;
  unlike assert_peer_receive, it keeps the ones before `last` for inspection.
  """
  @spec receive_until(map(), term(), integer(), [term()]) :: [term()]
  def receive_until(peer, last, timeout \\ 5_000, acc \\ []) do
    case remote_run(peer, do: EchoPubSub.TestSubscriber.get_message()) do
      ^last ->
        Enum.reverse([last | acc])

      nil when timeout > 0 ->
        Process.sleep(10)
        receive_until(peer, last, timeout - 10, acc)

      nil ->
        ExUnit.Assertions.flunk(
          "never received #{inspect(last)}, got: #{inspect(Enum.reverse(acc))}"
        )

      message ->
        receive_until(peer, last, timeout, [message | acc])
    end
  end
end
