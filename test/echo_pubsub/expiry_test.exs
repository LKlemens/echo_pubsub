defmodule EchoPubSub.ExpiryTest do
  use EchoPubSub.ClusterCase, async: true

  setup :start_peers

  test "gets 'expired' message when read cursor is too old", %{peer1: peer1, peer2: peer2} do
    remote_run peer2, node: peer1.node do
      Node.disconnect(node)
    end

    remote_run peer1 do
      for i <- 1..11 do
        Phoenix.PubSub.broadcast!(PubSubTest, "topic", i)
      end
    end

    remote_run peer2, node: peer1.node do
      Node.connect(node)
    end

    node = peer1.node
    assert_peer_receive peer2, {:cursor_expired, ^node}
  end

  test "workers get 'expired' if producer loses state", %{peer1: peer1, peer2: peer2} do
    remote_run peer2, node: peer1.node do
      Node.disconnect(node)
    end

    remote_run peer1 do
      GenServer.stop(PubSubTest.Adapter.Producer, :normal)
    end

    remote_run peer2, node: peer1.node do
      Node.connect(node)
    end

    node = peer1.node
    assert_peer_receive peer2, {:cursor_expired, ^node}
  end

  test "each subscriber gets one 'expired' message however many share its topic", %{
    peer1: peer1,
    peer2: peer2
  } do
    # 1. A second subscriber to "topic" on peer2, next to TestSubscriber.
    remote_run peer2 do
      parent = self()

      spawn(fn ->
        Phoenix.PubSub.subscribe(PubSubTest, "topic")
        send(parent, :subscribed)
        Process.sleep(:infinity)
      end)

      receive do
        :subscribed -> :ok
      end
    end

    # 2. peer1's producer restarts, so peer2 is told it may have missed messages.
    remote_run peer1, do: GenServer.stop(PubSubTest.Adapter.Producer, :normal)

    # 3. TestSubscriber gets the notice exactly once.
    node = peer1.node
    assert_peer_receive peer2, {:cursor_expired, ^node}
    refute_peer_receive peer2, {:cursor_expired, ^node}
  end

  test "flush path expires a lagging node instead of replaying overwritten messages",
       %{peer1: peer1, peer2: peer2} do
    node = peer1.node

    # Reject deliveries so peer2's read cursor stays pinned at 0 while writes continue.
    remote_run peer2, do: Application.put_env(:echo_pubsub, :fault_injection, :error)

    # buffer_size is 10; 12 messages push peer2's pinned cursor off the ring buffer.
    remote_run peer1 do
      for i <- 1..12 do
        Phoenix.PubSub.broadcast!(PubSubTest, "topic", {:overflow, i})
      end
    end

    # Recover. The flush path must signal expiry, not replay the overwritten slots
    # (which would surface as the wrapped-around {:overflow, 11} appearing first).
    remote_run peer2, do: Application.put_env(:echo_pubsub, :fault_injection, :ok)

    assert_peer_receive peer2, {:cursor_expired, ^node}
    refute_peer_receive peer2, {:overflow, 11}
  end

  test "retries the expired notice until the remote worker accepts it",
       %{peer1: peer1, peer2: peer2} do
    node = peer1.node

    # Reject everything on peer2 — batches AND the expired notice itself.
    remote_run peer2, do: Application.put_env(:echo_pubsub, :fault_injection, :error)

    # buffer_size is 10; 12 messages push peer2's pinned cursor off the ring buffer,
    # so the flush path starts trying to deliver {:cursor_expired, node} and fails.
    remote_run peer1 do
      for i <- 1..12 do
        Phoenix.PubSub.broadcast!(PubSubTest, "topic", {:overflow, i})
      end
    end

    # The failed notice must not be dropped: nothing arrives while faults are on...
    refute_peer_receive peer2, {:cursor_expired, ^node}

    # ...and once the worker accepts calls again, the retried notice lands.
    remote_run peer2, do: Application.put_env(:echo_pubsub, :fault_injection, :ok)
    assert_peer_receive peer2, {:cursor_expired, ^node}

    # The producer survived the failing calls and resumed normal delivery.
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :after_recovery)
    assert_peer_receive peer2, :after_recovery
  end
end
