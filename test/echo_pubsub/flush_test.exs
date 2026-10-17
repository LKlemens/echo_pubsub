defmodule EchoPubSub.FlushTest do
  use EchoPubSub.ClusterCase, async: true

  setup :start_peers

  test "does not send duplicated messages to the broadcasting node", %{peer1: peer1, peer2: peer2} do
    # peer1 subscribes to the same topic it will broadcast to
    remote_run peer1, do: EchoPubSub.TestSubscriber.subscribe(PubSubTest, "topic")

    # peer1 broadcasts a message
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :self_message)

    # peer2 should receive the message (already subscribed in setup)
    assert_peer_receive peer2, :self_message

    # peer1 should receive exactly one message (from local PubSub, not duplicated via producer)
    assert_peer_receive peer1, :self_message
    refute_peer_receive peer1, :self_message
  end

  test "local node cursor advances to write_cursor after flush", %{peer1: peer1} do
    # Subscribe locally on peer1
    remote_run peer1, do: EchoPubSub.TestSubscriber.subscribe(PubSubTest, "topic")

    # Send multiple messages from peer1
    for i <- 1..5 do
      remote_run peer1, i: i do
        Phoenix.PubSub.broadcast!(PubSubTest, "topic", {:local_cursor_test, i})
      end
    end

    # Check Producer state directly - local node's read_cursor should equal write_cursor
    assert_wait_for(fn ->
      {local_cursor, write_cursor} =
        remote_run peer1 do
          state = :sys.get_state(PubSubTest.Adapter.Producer)
          {Map.get(state.read_cursors, node()), state.write_cursor}
        end

      assert local_cursor == write_cursor,
             "local cursor (#{local_cursor}) should equal write_cursor (#{write_cursor})"
    end)
  end

  test "flush delivers to every remote node", %{peer1: peer1, peer2: peer2} do
    peer3 = spawn_node("node3", [peer1])
    remote_run peer3, do: EchoPubSub.TestSubscriber.subscribe(PubSubTest, "topic")

    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :fan_out)

    assert_peer_receive peer2, :fan_out
    assert_peer_receive peer3, :fan_out
  end
end
