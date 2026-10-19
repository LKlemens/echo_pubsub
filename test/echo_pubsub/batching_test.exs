defmodule EchoPubSub.BatchingTest do
  use EchoPubSub.ClusterCase, async: true

  # Flushes are sent by hand (the timer never fires) so the batch window a new
  # node joins during is deterministic.
  test "a new node gets the unflushed batch window but nothing flushed before it joined" do
    # 1. Two nodes; flushes only happen when sent by hand.
    [peer1, peer2] = spawn_nodes(["window_node1", "window_node2"], batch_interval: 60_000)
    remote_run peer2, do: EchoPubSub.TestSubscriber.subscribe(PubSubTest, "topic")
    assert_registered(peer2, peer1)

    # 2. m1 is flushed before peer3 exists.
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :m1)
    remote_run peer1, do: send(PubSubTest.Adapter.Producer, :flush_all)
    assert_peer_receive peer2, :m1

    # 3. m2 waits in the unflushed window.
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :m2)

    # 4. peer3 joins and registers.
    peer3 = spawn_node("window_node3", [peer1, peer2], batch_interval: 60_000)
    remote_run peer3, do: EchoPubSub.TestSubscriber.subscribe(PubSubTest, "topic")

    assert_registered(peer3, peer1)

    # 5. m3 lands after registration, then flush.
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :m3)
    remote_run peer1, do: send(PubSubTest.Adapter.Producer, :flush_all)

    # 6. peer3 gets the window (m2, m3) but not m1.
    assert_peer_receive peer3, :m2
    assert_peer_receive peer3, :m3
    refute_peer_receive peer3, :m1
  end

  # peer1's outgoing calls fail while the fault is on, so peer2's registration
  # keeps failing while m1 and m2 are flushed past it.
  test "a failed registration retries from its first start cursor" do
    # 1. peer1's outgoing calls fail.
    [peer1] = spawn_nodes(["retry_node1"], batch_interval: 60_000)
    remote_run peer1, do: Application.put_env(:echo_pubsub, :fault_injection, :error)

    # 2. peer2 joins; its first registration fails with start cursor 0.
    peer2 = spawn_node("retry_node2", [peer1], batch_interval: 60_000)
    remote_run peer2, do: EchoPubSub.TestSubscriber.subscribe(PubSubTest, "topic")

    assert_wait_for(5_000, fn ->
      registering =
        remote_run peer1 do
          :sys.get_state(PubSubTest.Adapter.Producer).registering
        end

      assert {nil, 0, _pid} = registering[peer2.node]
    end)

    # 3. m1, m2 are flushed past peer2 while it is unregistered.
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :m1)
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :m2)
    remote_run peer1, do: send(PubSubTest.Adapter.Producer, :flush_all)

    # 4. Fault cleared; the retry registers peer2, then flush.
    remote_run peer1, do: Application.put_env(:echo_pubsub, :fault_injection, :ok)
    assert_registered(peer2, peer1)
    remote_run peer1, do: send(PubSubTest.Adapter.Producer, :flush_all)

    # 5. peer2 still gets m1 and m2.
    assert_peer_receive peer2, :m1
    assert_peer_receive peer2, :m2
  end

  test "messages broadcast within one batch window are sent together" do
    # 1. Two nodes; the batch timer never fires, so flushes happen by hand.
    [peer1, peer2] = spawn_nodes(["batch_node1", "batch_node2"], batch_interval: 60_000)
    remote_run peer2, do: EchoPubSub.TestSubscriber.subscribe(PubSubTest, "topic")
    assert_registered(peer2, peer1)

    # 2. Both messages are held until the flush.
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :batch_msg1)
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :batch_msg2)
    refute_peer_receive peer2, :batch_msg1
    refute_peer_receive peer2, :batch_msg2

    # 3. One flush delivers both.
    remote_run peer1, do: send(PubSubTest.Adapter.Producer, :flush_all)
    assert_peer_receive peer2, :batch_msg1
    assert_peer_receive peer2, :batch_msg2
  end

  test "each batch sent to a node emits [:echo_pubsub, :sync, :batch] with its size and node" do
    # 1. Two nodes; the batch timer never fires, so the flush is sent by hand.
    [peer1, peer2] = spawn_nodes(["event_node1", "event_node2"], batch_interval: 60_000)
    remote_run peer2, do: EchoPubSub.TestSubscriber.subscribe(PubSubTest, "topic")
    assert_registered(peer2, peer1)

    remote_run peer1 do
      batches = :counters.new(2, [])
      :persistent_term.put(:batch_counters, batches)

      :telemetry.attach(
        "batch-events",
        [:echo_pubsub, :sync, :batch],
        fn _, m, meta, ref ->
          :counters.add(ref, 1, m.count)
          :counters.add(ref, 2, m.batch_size)
          :persistent_term.put(:batch_node, meta.node)
        end,
        batches
      )
    end

    # 2. Three writes, one flush: one batch of three to peer2.
    for i <- 1..3,
        do:
          remote_run(peer1, [i: i],
            do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", {:event, i})
          )

    remote_run peer1, do: send(PubSubTest.Adapter.Producer, :flush_all)
    assert_peer_receive peer2, {:event, 3}

    counts =
      remote_run peer1 do
        batches = :persistent_term.get(:batch_counters)
        {:counters.get(batches, 1), :counters.get(batches, 2), :persistent_term.get(:batch_node)}
      end

    assert counts == {1, 3, peer2.node}
  end
end
