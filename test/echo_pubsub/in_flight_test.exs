defmodule EchoPubSub.InFlightTest do
  use EchoPubSub.ClusterCase, async: true

  setup_all :start_shared_peers
  setup :reset_peers

  # A suspended worker accepts the delivery call but never answers it, so the
  # producer's call runs the full :call_timeout - an unresponsive peer that never
  # drops out of the cluster, which no monitor can report.
  test "producer keeps serving writes while a peer is unresponsive", %{
    peer1: peer1,
    peer2: peer2
  } do
    remote_run peer2, do: :sys.suspend(PubSubTest.Adapter.Worker)

    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :stalled)

    elapsed_ms =
      remote_run peer1 do
        {micros, :ok} =
          :timer.tc(fn -> Phoenix.PubSub.broadcast!(PubSubTest, "topic", :not_blocked) end)

        div(micros, 1000)
      end

    assert elapsed_ms < 1000,
           "broadcast blocked for #{elapsed_ms}ms behind an unresponsive peer"

    remote_run peer2, do: :sys.resume(PubSubTest.Adapter.Worker)

    assert_peer_receive peer2, :stalled
    assert_peer_receive peer2, :not_blocked
  end

  test "writes queued behind an in-flight batch arrive in order", %{peer1: peer1, peer2: peer2} do
    remote_run peer2, do: :sys.suspend(PubSubTest.Adapter.Worker)

    for n <- 1..5 do
      remote_run peer1, n: n do
        Phoenix.PubSub.broadcast!(PubSubTest, "topic", n)
      end
    end

    remote_run peer2, do: :sys.resume(PubSubTest.Adapter.Worker)

    for n <- 1..5, do: assert_peer_receive(peer2, ^n)
  end

  test "writes behind an in-flight batch are flushed once its verdict arrives", %{
    peer1: peer1,
    peer2: peer2
  } do
    remote_run peer2, do: :sys.suspend(PubSubTest.Adapter.Worker)

    # :follow1 is dispatched and stalls on the suspended worker. The next two are
    # buffered, and every flush while that batch is in flight skips peer2.
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :follow1)
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :follow2)
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :follow3)

    remote_run peer2, do: :sys.resume(PubSubTest.Adapter.Worker)

    # No further broadcast: the verdict for :follow1 is the only thing that can
    # schedule the flush carrying :follow2 and :follow3.
    assert_peer_receive peer2, :follow1
    assert_peer_receive peer2, :follow2
    assert_peer_receive peer2, :follow3
  end

  test "a write landing mid-delivery is not acked by the in-flight batch", %{
    peer1: peer1,
    peer2: peer2
  } do
    remote_run peer2, do: :sys.suspend(PubSubTest.Adapter.Worker)

    # The first write is dispatched and stalls; the second only reaches the buffer.
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :first)
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :second)

    {target, write_cursor} =
      remote_run peer1, peer2_node: peer2.node do
        state = :sys.get_state(PubSubTest.Adapter.Producer)
        {_ref, target} = Map.fetch!(state.in_flight, peer2_node)
        {target, state.write_cursor}
      end

    assert target == 1
    assert write_cursor == 2

    remote_run peer2, do: :sys.resume(PubSubTest.Adapter.Worker)

    assert_peer_receive peer2, :first
    assert_peer_receive peer2, :second
  end
end
