defmodule EchoPubSub.ClusterCase do
  @moduledoc """
  Case template for tests that run against a cluster of peer nodes.

  Each module sets up its own peers, either with `setup :start_peers` or by
  spawning them in its tests.
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
  Starts two connected peers, `peer1` and `peer2`, with `peer2` subscribed to
  "topic". Use as `setup :start_peers`.
  """
  @spec start_peers(map()) :: %{peer1: map(), peer2: map()}
  def start_peers(_context) do
    # Expiry/overflow tests intentionally fill the buffer; capacity warnings are
    # exercised separately, so disable them here to keep test output clean.
    [peer1, peer2] = spawn_nodes(["node1", "node2"], capacity_warning_threshold: 2.0)

    remote_run peer2 do
      EchoPubSub.TestSubscriber.subscribe(PubSubTest, "topic")
    end

    %{peer1: peer1, peer2: peer2}
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
end
