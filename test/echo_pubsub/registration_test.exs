defmodule EchoPubSub.RegistrationTest do
  use EchoPubSub.ClusterCase, async: true

  setup_all :start_shared_peers
  setup :reset_peers

  test "registers a known node again when its worker restarts", %{peer1: peer1, peer2: peer2} do
    # 1. peer2's worker, already registered by peer1, restarts with a new pid.
    assert_registered(peer2, peer1)

    old_worker =
      remote_run peer2 do
        pid = Process.whereis(PubSubTest.Adapter.Worker)
        Process.exit(pid, :kill)
        pid
      end

    # 2. peer1's producer registers the new worker.
    assert_wait_for(5_000, fn ->
      new_worker =
        remote_run peer2, old_worker: old_worker do
          pid = Process.whereis(PubSubTest.Adapter.Worker)
          if pid != old_worker, do: pid
        end

      workers =
        remote_run peer1 do
          :sys.get_state(PubSubTest.Adapter.Producer).workers
        end

      assert new_worker && Map.get(workers, peer2.node) == new_worker
    end)

    # 3. peer1's producer restarts; the new worker knows it, so peer2 is told.
    remote_run peer1, do: GenServer.stop(PubSubTest.Adapter.Producer, :normal)

    node = peer1.node
    assert_peer_receive peer2, {:cursor_expired, ^node}
  end

  test "a restarted producer serves writes while a peer hangs on registration", %{
    peer1: peer1,
    peer2: peer2
  } do
    # 1. peer2 hangs on every call.
    remote_run peer2, do: :sys.suspend(PubSubTest.Adapter.Worker)

    # 2. Restart peer1's producer; it must register with the hung peer2.
    remote_run peer1 do
      old = Process.whereis(PubSubTest.Adapter.Producer)
      GenServer.stop(old, :normal)

      # Wait for the supervisor to start the replacement.
      Enum.find_value(1..100, fn _ ->
        Process.sleep(10)
        new = Process.whereis(PubSubTest.Adapter.Producer)
        new != old && new
      end)
    end

    # 3. A broadcast still returns at once.
    elapsed_ms =
      remote_run peer1 do
        {micros, :ok} =
          :timer.tc(fn -> Phoenix.PubSub.broadcast!(PubSubTest, "topic", :after_restart) end)

        div(micros, 1000)
      end

    assert elapsed_ms < 1000,
           "broadcast blocked for #{elapsed_ms}ms behind a peer hung on registration"

    # 4. Once peer2 answers: cursor_expired first, then the message.
    remote_run peer2, do: :sys.resume(PubSubTest.Adapter.Worker)

    node = peer1.node
    assert_peer_receive peer2, {:cursor_expired, ^node}
    assert_peer_receive peer2, :after_restart
  end

  test "forgets a departed node once it falls off the buffer", %{peer1: peer1, peer2: peer2} do
    # 1. peer2 leaves peer1's :pg view.
    remote_run peer2, node: peer1.node do
      Node.disconnect(node)
    end

    # 2. buffer_size is 10; 12 messages put peer2 more than a buffer behind.
    remote_run peer1 do
      for i <- 1..12, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", i)
    end

    # 3. peer1's producer has forgotten peer2.
    assert_wait_for(fn ->
      read_cursors =
        remote_run peer1 do
          :sys.get_state(PubSubTest.Adapter.Producer).read_cursors
        end

      refute Map.has_key?(read_cursors, peer2.node)
    end)

    # 4. peer2 comes back: its worker still knows peer1, so it reports the gap,
    #    then gets new messages.
    remote_run peer2, node: peer1.node do
      Node.connect(node)
    end

    node = peer1.node
    assert_peer_receive peer2, {:cursor_expired, ^node}
    refute_peer_receive peer2, {:cursor_expired, ^node}

    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :after_return)
    assert_peer_receive peer2, :after_return
  end

  test "a forgotten node gets cursor_expired even after its worker restarts", %{
    peer1: peer1,
    peer2: peer2
  } do
    # 1. peer2 leaves peer1's :pg view.
    assert_registered(peer2, peer1)

    remote_run peer2, node: peer1.node do
      Node.disconnect(node)
    end

    # 2. While away, peer2's worker restarts; its subscribers stay up.
    remote_run peer2 do
      Process.exit(Process.whereis(PubSubTest.Adapter.Worker), :kill)
    end

    # 3. peer2 falls more than a buffer behind and peer1 forgets it.
    remote_run peer1 do
      for i <- 1..12, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", i)
    end

    assert_wait_for(fn ->
      read_cursors =
        remote_run peer1 do
          :sys.get_state(PubSubTest.Adapter.Producer).read_cursors
        end

      refute Map.has_key?(read_cursors, peer2.node)
    end)

    # 4. peer2 comes back: the new worker still knows peer1, so it reports the gap
    #    once, then gets new messages.
    remote_run peer2, node: peer1.node do
      Node.connect(node)
    end

    node = peer1.node
    assert_peer_receive peer2, {:cursor_expired, ^node}
    refute_peer_receive peer2, {:cursor_expired, ^node}

    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :after_return)
    assert_peer_receive peer2, :after_return
  end
end
