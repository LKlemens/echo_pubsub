defmodule EchoPubSub.SlowWorkerTest do
  use EchoPubSub.ClusterCase, async: true

  alias EchoPubSub.Producer.State

  @call_timeout 200
  @messages 40
  # 40 messages at 25 ms is a 1 s backlog: five times the call timeout.
  @cost_ms 25

  # Recovery used to send the whole backlog in one call. A worker that needs more
  # than call_timeout to fan it out never acked, so the same backlog was resent,
  # and the cursor never moved.
  test "a worker too slow for the whole backlog still catches up" do
    # 1. peer2 pays @cost_ms per message to deliver to its subscribers.
    [peer1, peer2] =
      spawn_nodes(["slow_node1", "slow_node2"],
        call_timeout: @call_timeout,
        buffer_size: 100,
        capacity_warning_threshold: 2.0
      )

    remote_run peer2 do
      EchoPubSub.TestSubscriber.subscribe(PubSubTest, "topic")
      Application.put_env(:echo_pubsub, :slow_dispatch_ms, unquote(@cost_ms))
    end

    assert_registered(peer2, peer1)

    # 2. The backlog builds while peer2's worker is hung.
    remote_run peer2, do: :sys.suspend(PubSubTest.Adapter.Worker)

    remote_run peer1, messages: @messages do
      for i <- 1..messages do
        Phoenix.PubSub.broadcast!(PubSubTest, "topic", i, EchoPubSub.SlowDispatcher)
      end
    end

    Process.sleep(@call_timeout * 2)
    remote_run peer2, do: :sys.resume(PubSubTest.Adapter.Worker)

    # 3. Every message arrives, and peer1 sees peer2 acknowledge all of them.
    received = receive_until(peer2, @messages, 20_000)
    assert MapSet.new(1..@messages) == MapSet.new(received)

    assert_wait_for(20_000, fn ->
      state = remote_run(peer1, do: :sys.get_state(PubSubTest.Adapter.Producer))
      assert state.read_cursors[peer2.node] == state.write_cursor
      assert state.write_cursor == @messages
      refute State.suspect?(state, peer2.node)
    end)
  end
end
