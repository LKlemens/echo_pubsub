defmodule EchoPubSub.RegistrationTest do
  use EchoPubSub.ClusterCase, async: true

  setup_all :start_shared_peers
  setup :reset_peers

  test "no message is lost while a worker is down", %{peer1: peer1, peer2: peer2} do
    # 1. A subscriber on peer1; both producers have registered the other peer.
    remote_run peer1, do: EchoPubSub.TestSubscriber.subscribe(PubSubTest, "topic")
    assert_registered(peer1, peer2)
    assert_registered(peer2, peer1)

    # 2. peer1's worker stops and stays down (a kill would restart it at once).
    remote_run peer1 do
      :ok =
        Supervisor.terminate_child(PubSubTest.Adapter.WorkerSupervisor, PubSubTest.Adapter.Worker)
    end

    # 3. A local broadcast never goes through the worker: peer1 gets it at once.
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :local_while_down)
    assert_peer_receive peer1, :local_while_down
    assert_peer_receive peer2, :local_while_down

    # 4. peer2's message waits in peer2's buffer while peer1 has no worker.
    remote_run peer2, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :remote_while_down)
    refute_peer_receive peer1, :remote_while_down

    # 5. The worker is back: peer2 resumes it and replays the message, no notice.
    remote_run peer1 do
      {:ok, _pid} =
        Supervisor.restart_child(PubSubTest.Adapter.WorkerSupervisor, PubSubTest.Adapter.Worker)
    end

    messages = receive_until(peer1, :remote_while_down)
    refute Enum.any?(messages, &match?({:cursor_expired, _}, &1))
  end

  test "a batch lost in a crashing worker is redelivered to its replacement", %{
    peer1: peer1,
    peer2: peer2
  } do
    # 1. A subscriber on peer1; peer2 has registered peer1's worker.
    remote_run peer1, do: EchoPubSub.TestSubscriber.subscribe(PubSubTest, "topic")
    assert_registered(peer1, peer2)

    # 2. peer1's worker stops handling messages, so the next batch waits in its mailbox.
    remote_run peer1, do: :sys.suspend(PubSubTest.Adapter.Worker)

    # 3. peer2's batch reaches that mailbox and its delivery stays in flight.
    remote_run peer2, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :in_flight)

    assert_wait_for(fn ->
      in_flight =
        remote_run peer2 do
          :sys.get_state(PubSubTest.Adapter.Producer).in_flight
        end

      assert Map.has_key?(in_flight, peer1.node)
    end)

    # 4. The worker crashes with the batch unread; the supervisor starts a new one.
    remote_run peer1, do: Process.exit(Process.whereis(PubSubTest.Adapter.Worker), :kill)

    # 5. peer2 kept peer1's cursor, so it resumes the new worker and re-sends the
    #    batch, with no notice before it.
    messages = receive_until(peer1, :in_flight)
    refute Enum.any?(messages, &match?({:cursor_expired, _}, &1))
  end

  test "a worker down for more than a buffer gets cursor_expired", %{
    peer1: peer1,
    peer2: peer2
  } do
    # 1. A subscriber on peer1; peer2 has registered peer1's worker.
    remote_run peer1, do: EchoPubSub.TestSubscriber.subscribe(PubSubTest, "topic")
    assert_registered(peer1, peer2)

    # 2. peer1's worker stops and stays down; peer1 stays connected.
    remote_run peer1 do
      :ok =
        Supervisor.terminate_child(PubSubTest.Adapter.WorkerSupervisor, PubSubTest.Adapter.Worker)
    end

    # 3. buffer_size is 10; 12 messages overwrite some peer1 never got, so
    #    peer2's producer forgets peer1.
    remote_run peer2 do
      for i <- 1..12, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", i)
    end

    assert_wait_for(fn ->
      read_cursors =
        remote_run peer2 do
          :sys.get_state(PubSubTest.Adapter.Producer).read_cursors
        end

      refute Map.has_key?(read_cursors, peer1.node)
    end)

    # 4. The worker is back; it still remembers peer2, so it reports the gap.
    remote_run peer1 do
      {:ok, _pid} =
        Supervisor.restart_child(PubSubTest.Adapter.WorkerSupervisor, PubSubTest.Adapter.Worker)
    end

    node = peer2.node
    assert_peer_receive peer1, {:cursor_expired, ^node}

    # 5. New messages from peer2 arrive again.
    remote_run peer2, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :after_return)
    assert_peer_receive peer1, :after_return
  end

  test "a restarted worker gets no cursor_expired from its own producer or a peer", %{
    peer1: peer1,
    peer2: peer2
  } do
    # 1. A subscriber on peer1; peer2 has registered peer1's worker.
    remote_run peer1, do: EchoPubSub.TestSubscriber.subscribe(PubSubTest, "topic")
    assert_registered(peer1, peer2)

    # 2. peer1's worker restarts; wait until peer2's producer has the new one.
    old_worker =
      remote_run peer1 do
        pid = Process.whereis(PubSubTest.Adapter.Worker)
        Process.exit(pid, :kill)
        pid
      end

    assert_wait_for(5_000, fn ->
      new_worker =
        remote_run peer1, old_worker: old_worker do
          pid = Process.whereis(PubSubTest.Adapter.Worker)
          if pid != old_worker, do: pid
        end

      workers =
        remote_run peer2 do
          :sys.get_state(PubSubTest.Adapter.Producer).workers
        end

      assert new_worker && Map.get(workers, peer1.node) == new_worker
    end)

    # 3. Neither producer lost anything, so no notice reaches peer1.
    refute_peer_receive peer1, {:cursor_expired, _}

    # 4. Messages from both producers still arrive.
    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :from_peer1)
    remote_run peer2, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :from_peer2)
    assert_peer_receive peer1, :from_peer1
    assert_peer_receive peer1, :from_peer2
  end

  test "a crash-looping worker leaves its producer and buffer alone", %{
    peer1: peer1,
    peer2: peer2
  } do
    # 1. peer2 is subscribed and registered with peer1's producer.
    assert_registered(peer2, peer1)
    producer = remote_run(peer1, do: Process.whereis(PubSubTest.Adapter.Producer))

    # 2. peer1's worker crashes five times in a row, past the restart limit of
    #    its own supervisor.
    for _ <- 1..5 do
      old_worker =
        remote_run peer1 do
          pid = Process.whereis(PubSubTest.Adapter.Worker)
          Process.exit(pid, :kill)
          pid
        end

      assert_wait_for(fn ->
        new_worker =
          remote_run(peer1, do: Process.whereis(PubSubTest.Adapter.Worker))

        assert new_worker && new_worker != old_worker
      end)
    end

    # 3. The producer kept running, so peer2 is told nothing and still gets
    #    peer1's broadcasts.
    assert remote_run(peer1, do: Process.whereis(PubSubTest.Adapter.Producer)) == producer

    remote_run peer1, do: Phoenix.PubSub.broadcast!(PubSubTest, "topic", :after_crashes)
    messages = receive_until(peer2, :after_crashes)
    refute Enum.any?(messages, &match?({:cursor_expired, _}, &1))
  end

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

  # Every message the peer's subscriber gets up to and including `last`, in order;
  # unlike assert_peer_receive, it keeps the ones before `last` for inspection.
  defp receive_until(peer, last, timeout \\ 5_000, acc \\ []) do
    case remote_run(peer, do: EchoPubSub.TestSubscriber.get_message()) do
      ^last ->
        Enum.reverse([last | acc])

      nil when timeout > 0 ->
        Process.sleep(10)
        receive_until(peer, last, timeout - 10, acc)

      nil ->
        flunk("never received #{inspect(last)}, got: #{inspect(Enum.reverse(acc))}")

      message ->
        receive_until(peer, last, timeout, [message | acc])
    end
  end
end
