defmodule PhoenixPubSubBufferedTest do
  use ExUnit.Case

  import PhoenixPubSubBuffered.Cluster

  doctest PhoenixPubSubBuffered

  setup do
    [peer1, peer2] = spawn_nodes(["node1", "node2"])

    remote_run peer2 do
      PhoenixPubSubBuffered.TestSubscriber.subscribe(PubSubTest, "topic")
    end

    %{peer1: peer1, peer2: peer2}
  end

  test "receives broadcast message on all connected nodes", %{peer1: peer1, peer2: peer2} do
    peer3 = spawn_node("node3", [peer1])
    remote_run peer3, do: PhoenixPubSubBuffered.TestSubscriber.subscribe(PubSubTest, "topic")
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :message)
    assert_peer_receive peer2, :message
    assert_peer_receive peer3, :message
  end

  test "direct_broadcast targets a specific node", %{peer1: peer1, peer2: peer2} do
    peer3 = spawn_node("node3", [peer1])
    remote_run peer3, do: PhoenixPubSubBuffered.TestSubscriber.subscribe(PubSubTest, "topic")

    remote_run peer1, node: peer3.node do
      Phoenix.PubSub.direct_broadcast!(node, PubSubTest, "topic", :message)
    end

    refute_peer_receive peer2, :message
    assert_peer_receive peer3, :message
  end

  test "catches up with messages after disconnect", %{peer1: peer1, peer2: peer2} do
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", 1)
    assert_peer_receive peer2, 1

    remote_run peer2, node: peer1.node do
      Node.disconnect(node)
    end

    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", 2)
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", 3)

    remote_run peer2, node: peer1.node do
      Node.connect(node)
    end

    assert_peer_receive peer2, 2
    assert_peer_receive peer2, 3
  end

  test "new client only reads new messages", %{peer1: peer1} do
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", 1)
    peer3 = spawn_node("node3", [peer1])

    remote_run peer3 do
      PhoenixPubSubBuffered.TestSubscriber.subscribe(PubSubTest, "topic")
    end

    remote_run(peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", 2))
    assert_peer_receive peer3, 2
  end

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

  test "messages are batched together within 200ms window" do
    # spawn nodes with batch_interval: 200 for batching behavior
    [peer1, peer2] = spawn_nodes(["batch_node1", "batch_node2"], batch_interval: 200)

    remote_run peer2 do
      PhoenixPubSubBuffered.TestSubscriber.subscribe(PubSubTest, "topic")
    end

    # send messages rapidly (within 200ms window)
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :batch_msg1)
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :batch_msg2)

    # wait for batch to arrive
    assert_peer_receive peer2, :batch_msg1
    assert_peer_receive peer2, :batch_msg2

    # verify messages were sent in a single batch
    last_batch =
      remote_run peer2 do
        GenServer.call(PubSubTest.Adapter.Worker, :get_last_batch)
      end

    assert length(last_batch) >= 2, "Expected batched messages, got: #{inspect(last_batch)}"
  end

  test "does not send duplicated messages to the broadcasting node", %{peer1: peer1, peer2: peer2} do
    # peer1 subscribes to the same topic it will broadcast to
    remote_run peer1, do: PhoenixPubSubBuffered.TestSubscriber.subscribe(PubSubTest, "topic")

    # peer1 broadcasts a message
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :self_message)

    # peer2 should receive the message (already subscribed in setup)
    assert_peer_receive peer2, :self_message

    # peer1 should receive exactly one message (from local PubSub, not duplicated via producer)
    assert_peer_receive peer1, :self_message
    refute_peer_receive peer1, :self_message
  end

  test "retries sending messages after temporary failure", %{peer1: peer1, peer2: peer2} do
    # Make peer2's worker reject messages
    remote_run peer2, do: Application.put_env(:msg, :val, :error)

    # Send a message - it should fail initially
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :retry_message)

    # Wait a bit, message should NOT be received yet
    refute_peer_receive peer2, :retry_message

    # Allow messages again
    remote_run peer2, do: Application.put_env(:msg, :val, :ok)

    # After retry (200ms), message should be received
    assert_peer_receive peer2, :retry_message
  end

  test "messages are preserved during failure and delivered after recovery", %{peer1: peer1, peer2: peer2} do
    # Make peer2's worker reject messages
    remote_run peer2, do: Application.put_env(:msg, :val, :error)

    # Send multiple messages
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :msg1)
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :msg2)
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :msg3)

    # Messages should NOT be received during failure
    refute_peer_receive peer2, :msg1

    # Allow messages again
    remote_run peer2, do: Application.put_env(:msg, :val, :ok)

    # All messages should be received after retry
    assert_peer_receive peer2, :msg1
    assert_peer_receive peer2, :msg2
    assert_peer_receive peer2, :msg3
  end

  test "message delivery succeeds after multiple retry cycles", %{peer1: peer1, peer2: peer2} do
    # Make peer2's worker reject messages
    remote_run peer2, do: Application.put_env(:msg, :val, :error)

    # Send a message
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :persistent_retry_msg)

    # Wait for multiple retry cycles (200ms each), still failing
    Process.sleep(500)
    refute_peer_receive peer2, :persistent_retry_msg

    # Allow messages again
    remote_run peer2, do: Application.put_env(:msg, :val, :ok)

    # Message should be received after next retry
    assert_peer_receive peer2, :persistent_retry_msg
  end
end
