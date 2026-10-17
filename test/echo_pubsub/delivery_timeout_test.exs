defmodule EchoPubSub.DeliveryTimeoutTest do
  use EchoPubSub.ClusterCase, async: true

  # A call timeout does not pull the request out of the hung worker's mailbox, so
  # every timed-out attempt runs on resume: delivery is at-least-once.
  test "re-sends a batch after its delivery call times out" do
    # Short timeout so the call gives up while the worker is still suspended.
    [peer1, peer2] =
      spawn_nodes(["timeout_node1", "timeout_node2"],
        call_timeout: 100,
        capacity_warning_threshold: 2.0
      )

    remote_run peer2, do: EchoPubSub.TestSubscriber.subscribe(PubSubTest, "topic")

    # The delivery call must be the one that hangs, not peer2's registration.
    assert_registered(peer2, peer1)

    # Hung but connected peer: calls queue up instead of failing fast.
    remote_run peer2, do: :sys.suspend(PubSubTest.Adapter.Worker)

    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :msg)

    # Stays hung across several 100ms timeouts; each 200ms retry queues another
    # copy of the batch in the worker's mailbox.
    Process.sleep(500)
    remote_run peer2, do: :sys.resume(PubSubTest.Adapter.Worker)

    # Every queued copy runs on resume, so :msg arrives several times. Two are
    # enough to prove the batch was re-sent; the exact count depends on timing.
    assert_peer_receive peer2, :msg
    assert_peer_receive peer2, :msg
  end
end
