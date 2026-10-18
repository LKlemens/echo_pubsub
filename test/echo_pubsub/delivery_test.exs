defmodule EchoPubSub.DeliveryTest do
  use EchoPubSub.ClusterCase, async: true

  setup_all :start_shared_peers
  setup :reset_peers

  doctest EchoPubSub

  test "receives broadcast message on all connected nodes", %{peer1: peer1, peer2: peer2} do
    peer3 = spawn_node("node3", [peer1])
    remote_run peer3, do: EchoPubSub.TestSubscriber.subscribe(PubSubTest, "topic")
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :message)
    assert_peer_receive peer2, :message
    assert_peer_receive peer3, :message
  end

  test "node_name is the node the adapter runs on", %{peer1: peer1} do
    assert remote_run(peer1, do: Phoenix.PubSub.node_name(PubSubTest)) == peer1.node
  end

  test "direct_broadcast targets a specific node", %{peer1: peer1, peer2: peer2} do
    peer3 = spawn_node("node3", [peer1])
    remote_run peer3, do: EchoPubSub.TestSubscriber.subscribe(PubSubTest, "topic")

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
      EchoPubSub.TestSubscriber.subscribe(PubSubTest, "topic")
    end

    remote_run(peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", 2))
    assert_peer_receive peer3, 2
  end
end
