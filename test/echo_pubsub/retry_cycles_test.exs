defmodule EchoPubSub.RetryCyclesTest do
  use EchoPubSub.ClusterCase, async: true

  setup :start_peers

  test "message delivery succeeds after multiple retry cycles", %{peer1: peer1, peer2: peer2} do
    # Make peer2's worker reject messages
    remote_run peer2, do: Application.put_env(:echo_pubsub, :fault_injection, :error)

    # Send a message
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :persistent_retry_msg)

    # Wait for multiple retry cycles (200ms each), still failing
    Process.sleep(500)
    refute_peer_receive peer2, :persistent_retry_msg

    # Allow messages again
    remote_run peer2, do: Application.put_env(:echo_pubsub, :fault_injection, :ok)

    # Message should be received after next retry
    assert_peer_receive peer2, :persistent_retry_msg
  end

  test "a failing peer does not hold back a healthy one", %{peer1: peer1, peer2: peer2} do
    peer3 = spawn_node("node3", [peer1])
    remote_run peer3, do: EchoPubSub.TestSubscriber.subscribe(PubSubTest, "topic")

    # peer3's worker rejects every batch; peer2's keeps accepting.
    remote_run peer3, do: Application.put_env(:echo_pubsub, :fault_injection, :error)

    for message <- [:iso1, :iso2, :iso3] do
      remote_run peer1, message: message do
        Phoenix.PubSub.broadcast!(PubSubTest, "topic", message)
      end
    end

    # The healthy peer is delivered on its own cursor, unaffected by peer3.
    assert_peer_receive peer2, :iso1
    assert_peer_receive peer2, :iso2
    assert_peer_receive peer2, :iso3

    # Several 200ms retry cycles pass with peer3 still rejecting.
    Process.sleep(500)
    refute_peer_receive peer3, :iso1

    remote_run peer3, do: Application.put_env(:echo_pubsub, :fault_injection, :ok)

    assert_peer_receive peer3, :iso1
    assert_peer_receive peer3, :iso2
    assert_peer_receive peer3, :iso3

    # Those retries replayed to peer3 only: peer2 acked the batch, so its cursor
    # moved and it is never re-sent what it already has.
    refute_peer_receive peer2, :iso1
  end
end
