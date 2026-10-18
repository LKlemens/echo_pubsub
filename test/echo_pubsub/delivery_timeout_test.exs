defmodule EchoPubSub.DeliveryTimeoutTest do
  use EchoPubSub.ClusterCase, async: true

  alias EchoPubSub.Producer.Remote

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
