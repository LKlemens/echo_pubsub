defmodule EchoPubSub.RegistrationRetryTest do
  use EchoPubSub.ClusterCase, async: true

  # Own module so its peers boot and hang in parallel with RegistrationTest.
  test "a registration retried against a hung worker notifies at most twice, not once per retry" do
    # Own peers with a short timeout, so registration attempts time out quickly.
    [peer1, peer2] =
      spawn_nodes(["hung_reg_node1", "hung_reg_node2"],
        call_timeout: 100,
        capacity_warning_threshold: 2.0
      )

    remote_run peer2, do: EchoPubSub.TestSubscriber.subscribe(PubSubTest, "topic")
    assert_registered(peer2, peer1)

    # 1. peer2 hangs; peer1's restarted producer must register with it, and peer2
    #    knows peer1, so every registration it handles sends a notice.
    remote_run peer2, do: :sys.suspend(PubSubTest.Adapter.Worker)
    old_producer = remote_run(peer1, do: Process.whereis(PubSubTest.Adapter.Producer))
    remote_run peer1, do: GenServer.stop(PubSubTest.Adapter.Producer, :normal)

    assert_wait_for(fn ->
      new_producer = remote_run(peer1, do: Process.whereis(PubSubTest.Adapter.Producer))
      assert new_producer && new_producer != old_producer
    end)

    # 2. Several 100ms timeouts and 50ms retries pass; each queues only a ping.
    Process.sleep(300)
    remote_run peer2, do: :sys.resume(PubSubTest.Adapter.Worker)

    # 3. The queued registration, then at most the one retried after the ping.
    assert_registered(peer2, peer1)
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :marker)
    messages = receive_until(peer2, :marker)
    notices = Enum.count(messages, &(&1 == {:cursor_expired, peer1.node}))

    assert notices in 1..2, "got #{notices} notices: #{inspect(messages)}"
  end
end
