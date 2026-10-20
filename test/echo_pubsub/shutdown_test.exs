defmodule EchoPubSub.ShutdownTest do
  use EchoPubSub.ClusterCase, async: true

  # The timer never fires (60 s), so only a flush on stop can deliver the write.
  test "a graceful stop flushes writes that were accepted but not sent" do
    [peer1, peer2] =
      spawn_nodes(["shutdown_node1", "shutdown_node2"],
        batch_interval: 60_000,
        capacity_warning_threshold: 2.0
      )

    remote_run peer2, do: EchoPubSub.TestSubscriber.subscribe(PubSubTest, "topic")
    assert_registered(peer2, peer1)

    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :accepted)
    refute_peer_receive(peer2, :accepted, 200)

    remote_run peer1, do: EchoPubSub.Cluster.stop_children()

    assert_peer_receive peer2, :accepted
  end

  # The producer traps exits now, so a stray EXIT arrives as a message.
  test "an unexpected EXIT message does not stop the producer" do
    [peer1] = spawn_nodes(["exit_node1"], capacity_warning_threshold: 2.0)

    {before, after_exit} =
      remote_run peer1 do
        producer = Process.whereis(PubSubTest.Adapter.Producer)
        send(producer, {:EXIT, self(), :boom})
        _ = :sys.get_state(producer)
        {producer, Process.whereis(PubSubTest.Adapter.Producer)}
      end

    assert before == after_exit
  end

  test "a kill still loses unsent writes: accepted is not durable" do
    [peer1, peer2] =
      spawn_nodes(["kill_node1", "kill_node2"],
        batch_interval: 60_000,
        capacity_warning_threshold: 2.0
      )

    remote_run peer2, do: EchoPubSub.TestSubscriber.subscribe(PubSubTest, "topic")
    assert_registered(peer2, peer1)

    remote_run peer1 do
      Phoenix.PubSub.broadcast!(PubSubTest, "topic", :lost)
      Process.exit(Process.whereis(PubSubTest.Adapter.Producer), :kill)
    end

    refute_peer_receive(peer2, :lost, 500)
  end

  test "a graceful stop waits at most call_timeout for a node that does not answer" do
    [peer1, peer2] =
      spawn_nodes(["hung_node1", "hung_node2"],
        batch_interval: 60_000,
        call_timeout: 300,
        capacity_warning_threshold: 2.0
      )

    remote_run peer2, do: EchoPubSub.TestSubscriber.subscribe(PubSubTest, "topic")
    assert_registered(peer2, peer1)
    remote_run peer2, do: :sys.suspend(PubSubTest.Adapter.Worker)

    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :stuck)

    {elapsed_us, :ok} =
      :timer.tc(fn -> remote_run(peer1, do: EchoPubSub.Cluster.stop_children()) end)

    assert elapsed_us < 1_000_000
  end
end
