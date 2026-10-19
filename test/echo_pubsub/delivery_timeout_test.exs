defmodule EchoPubSub.DeliveryTimeoutTest do
  use EchoPubSub.ClusterCase, async: true

  alias EchoPubSub.Producer.Remote
  alias EchoPubSub.Producer.State

  # A call timeout does not pull the request out of the hung worker's mailbox, so
  # the timed-out batch still runs on resume. The producer then pings instead of
  # resending, so the batch arrives twice, not once per retry.
  test "re-sends a batch once after its delivery call times out, not once per retry" do
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

    # Stays hung across several 100ms timeouts and 50ms retries; each retry
    # queues only a ping behind the first batch.
    Process.sleep(300)
    remote_run peer2, do: :sys.resume(PubSubTest.Adapter.Worker)

    # The queued batch, then the one resent after a ping is answered.
    assert_peer_receive peer2, :msg
    assert_peer_receive peer2, :msg
    refute_peer_receive(peer2, :msg, 300)
  end

  # The node already waited out its failure, so once it answers it must not wait
  # a whole batch window as well.
  test "a node that answers its ping is retried at once, not after batch_interval" do
    # 1. peer1's calls fail; its batch timer never fires within the test.
    [peer1] = spawn_nodes(["ping_node1"], batch_interval: 60_000)
    remote_run peer1, do: Application.put_env(:echo_pubsub, :fault_injection, :error)

    # 2. peer2's registration fails, so peer1 pings it before trying again.
    peer2 = spawn_node("ping_node2", [peer1], batch_interval: 60_000)

    assert_wait_for(5_000, fn ->
      state = remote_run(peer1, do: :sys.get_state(PubSubTest.Adapter.Producer))
      assert State.suspect?(state, peer2.node)
    end)

    # 3. Once the fault clears, the next ping succeeds and peer2 registers
    #    within a few retries.
    remote_run peer1, do: Application.put_env(:echo_pubsub, :fault_injection, :ok)

    assert_wait_for(1_000, fn ->
      read_cursors =
        remote_run(peer1, do: :sys.get_state(PubSubTest.Adapter.Producer).read_cursors)

      assert Map.has_key?(read_cursors, peer2.node)
    end)
  end

  describe "probe" do
    test "a worker that rejects :ping as unknown (older version) is alive" do
      pid = spawn_link(fn -> reply_once({:error, :bad_message}) end)
      assert Remote.probe(pid, 1_000) == :ok
    end

    test "a worker that never answers is not alive" do
      pid = spawn_link(fn -> Process.sleep(:infinity) end)
      assert Remote.probe(pid, 50) == :error
    end
  end

  defp reply_once(reply) do
    receive do
      {:"$gen_call", from, :ping} -> GenServer.reply(from, reply)
    end
  end
end
