defmodule EchoPubSub.TaskDownTest do
  use EchoPubSub.ClusterCase, async: true

  setup_all :start_shared_peers
  setup :reset_peers

  # Remote turns every remote failure into a result, so a task dies without one
  # only on a bug; killing it while its call hangs on a suspended worker stands in.
  test "a delivery task that dies without a verdict is retried", %{peer1: peer1, peer2: peer2} do
    # 1. peer2 hangs on every call; peer1's batch to it stays in flight.
    remote_run peer2, do: :sys.suspend(PubSubTest.Adapter.Worker)
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :lost_task)

    {old_ref, cursor} =
      assert_wait_for(fn ->
        state = producer_state(peer1)
        assert {ref, _target} = state.in_flight[peer2.node]
        {ref, state.read_cursors[peer2.node]}
      end)

    # 2. The task dies; the producer keeps the cursor and starts a new delivery.
    kill_task(peer1)

    assert_wait_for(fn ->
      state = producer_state(peer1)
      assert {ref, _target} = state.in_flight[peer2.node]
      assert ref != old_ref
      assert state.read_cursors[peer2.node] == cursor
    end)

    # 3. Once peer2 answers, the message arrives and the cursor catches up.
    remote_run peer2, do: :sys.resume(PubSubTest.Adapter.Worker)
    assert_peer_receive peer2, :lost_task

    assert_wait_for(fn ->
      state = producer_state(peer1)
      assert state.read_cursors[peer2.node] == state.write_cursor
    end)
  end

  test "a registration task that dies is retried", %{peer1: peer1, peer2: peer2} do
    # 1. peer2 hangs on every call; peer1's restarted producer must register with it.
    remote_run peer2, do: :sys.suspend(PubSubTest.Adapter.Worker)
    restart_producer(peer1)

    old_ref =
      assert_wait_for(fn ->
        assert {ref, _start, _pid} = producer_state(peer1).registering[peer2.node]
        assert is_reference(ref)
        ref
      end)

    # 2. The task dies; the producer tries the registration again.
    kill_task(peer1)

    assert_wait_for(fn ->
      assert {ref, _start, _pid} = producer_state(peer1).registering[peer2.node]
      assert is_reference(ref) and ref != old_ref
    end)

    # 3. Once peer2 answers, it is registered and gets new messages.
    remote_run peer2, do: :sys.resume(PubSubTest.Adapter.Worker)
    assert_registered(peer2, peer1)

    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :after_retry)
    assert_peer_receive peer2, :after_retry
  end

  defp producer_state(peer) do
    remote_run(peer, do: :sys.get_state(PubSubTest.Adapter.Producer))
  end

  defp kill_task(peer) do
    remote_run peer do
      [pid] = Task.Supervisor.children(PubSubTest.Adapter.TaskSupervisor)
      Process.exit(pid, :kill)
    end
  end

  defp restart_producer(peer) do
    remote_run peer do
      old = Process.whereis(PubSubTest.Adapter.Producer)
      GenServer.stop(old, :normal)

      Enum.find_value(1..100, fn _ ->
        Process.sleep(10)
        new = Process.whereis(PubSubTest.Adapter.Producer)
        new != old && new
      end)
    end
  end
end
