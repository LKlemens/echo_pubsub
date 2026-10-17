defmodule EchoPubSub.FaultRetryTest do
  use EchoPubSub.ClusterCase, async: true

  setup :start_peers

  test "retries sending messages after temporary failure", %{peer1: peer1, peer2: peer2} do
    # Make peer2's worker reject messages
    remote_run peer2, do: Application.put_env(:echo_pubsub, :fault_injection, :error)

    # Send a message - it should fail initially
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :retry_message)

    # Wait a bit, message should NOT be received yet
    refute_peer_receive peer2, :retry_message

    # Allow messages again
    remote_run peer2, do: Application.put_env(:echo_pubsub, :fault_injection, :ok)

    # After retry (200ms), message should be received
    assert_peer_receive peer2, :retry_message
  end

  test "messages are preserved during failure and delivered after recovery", %{
    peer1: peer1,
    peer2: peer2
  } do
    # Make peer2's worker reject messages
    remote_run peer2, do: Application.put_env(:echo_pubsub, :fault_injection, :error)

    # Send multiple messages
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :msg1)
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :msg2)
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :msg3)

    # Messages should NOT be received during failure
    refute_peer_receive peer2, :msg1

    # Allow messages again
    remote_run peer2, do: Application.put_env(:echo_pubsub, :fault_injection, :ok)

    # All messages should be received after retry
    assert_peer_receive peer2, :msg1
    assert_peer_receive peer2, :msg2
    assert_peer_receive peer2, :msg3
  end

  test "producer holds outgoing messages while the sending node is faulted", %{
    peer1: peer1,
    peer2: peer2
  } do
    # Fault the SENDING node: its producer must not deliver outward, the
    # mirror of the receive-side tests above.
    remote_run peer1, do: Application.put_env(:echo_pubsub, :fault_injection, :error)

    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :out1)
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :out2)

    # Nothing leaves peer1 while it is faulted - the batch stays buffered.
    refute_peer_receive peer2, :out1

    # Clear the fault; the buffered writes replay in order on the next retry.
    remote_run peer1, do: Application.put_env(:echo_pubsub, :fault_injection, :ok)

    assert_peer_receive peer2, :out1
    assert_peer_receive peer2, :out2
  end
end
