defmodule EchoPubSub.PropertyTest do
  @moduledoc """
  Plays random schedules of broadcasts and faults on three peers, heals the
  cluster, then checks the delivery guarantees:

    * every broadcast reaches every subscriber,
    * no gaps: message `n` from a producer arrives only after `1..n-1` did,
    * no `cursor_expired`: the buffer outlasts any schedule.

  Duplicates are allowed (at-least-once). Set `PROPERTY_RUNS` for a longer soak.
  """
  use EchoPubSub.ClusterCase, async: true
  use ExUnitProperties

  @runs String.to_integer(System.get_env("PROPERTY_RUNS", "25"))
  @peer_count 3

  setup_all do
    peers =
      spawn_nodes(["prop1", "prop2", "prop3"],
        buffer_size: 1_000,
        batch_interval: 10,
        call_timeout: 300,
        capacity_warning_threshold: 2.0
      )

    %{peers: List.to_tuple(peers)}
  end

  property "every broadcast reaches every subscriber in order, despite faults", %{peers: peers} do
    check all(schedule <- schedule(), max_runs: @runs, max_shrinking_steps: 20) do
      peer_list = Tuple.to_list(peers)
      start_clean(peer_list)

      run_id = make_ref()
      sent = play(schedule, peers, run_id)
      heal(peer_list)

      for peer <- peer_list do
        assert_wait_for(10_000, fn -> assert_delivered(peer, run_id, sent) end)
      end
    end
  end

  ## Schedule

  defp schedule do
    peer = integer(0..(@peer_count - 1))

    step =
      frequency([
        {6, tuple({constant(:broadcast), peer})},
        {2, tuple({constant(:wait), integer(0..50)})},
        {1, tuple({constant(:fault), peer})},
        {1, tuple({constant(:heal), peer})},
        {1, tuple({constant(:kill_worker), peer})},
        {1, tuple({constant(:suspend_worker), peer})},
        {1, tuple({constant(:resume_worker), peer})},
        {1, tuple({constant(:disconnect), peer, peer}) |> filter(fn {_, a, b} -> a != b end)}
      ])

    step
    |> list_of(min_length: 1, max_length: 30)
    |> filter(&within_restart_limit?/1)
  end

  # A fourth worker crash within 5 s exceeds the adapter supervisor's restart
  # limit and takes the producer down with it, losing its buffer.
  defp within_restart_limit?(schedule) do
    schedule
    |> Enum.filter(&match?({:kill_worker, _}, &1))
    |> Enum.frequencies()
    |> Enum.all?(fn {_kill, count} -> count <= 3 end)
  end

  # Plays each step and returns how many messages each producer index sent.
  defp play(schedule, peers, run_id) do
    Enum.reduce(schedule, %{}, fn step, sent -> play_step(step, peers, run_id, sent) end)
  end

  defp play_step({:broadcast, index}, peers, run_id, sent) do
    seq = Map.get(sent, index, 0) + 1
    message = {:prop, run_id, index, seq}

    remote_run elem(peers, index), message: message do
      Phoenix.PubSub.broadcast!(PubSubTest, "topic", message)
    end

    Map.put(sent, index, seq)
  end

  defp play_step({:wait, ms}, _peers, _run_id, sent) do
    Process.sleep(ms)
    sent
  end

  defp play_step({:fault, index}, peers, _run_id, sent) do
    remote_run elem(peers, index), do: Application.put_env(:echo_pubsub, :fault_injection, :error)
    sent
  end

  defp play_step({:heal, index}, peers, _run_id, sent) do
    remote_run elem(peers, index), do: Application.put_env(:echo_pubsub, :fault_injection, :ok)
    sent
  end

  defp play_step({:kill_worker, index}, peers, _run_id, sent) do
    remote_run elem(peers, index) do
      if pid = Process.whereis(PubSubTest.Adapter.Worker), do: Process.exit(pid, :kill)
    end

    sent
  end

  defp play_step({:suspend_worker, index}, peers, _run_id, sent) do
    remote_run elem(peers, index) do
      if pid = Process.whereis(PubSubTest.Adapter.Worker), do: :sys.suspend(pid)
    end

    sent
  end

  defp play_step({:resume_worker, index}, peers, _run_id, sent) do
    resume_worker(elem(peers, index))
    sent
  end

  defp play_step({:disconnect, a, b}, peers, _run_id, sent) do
    remote_run elem(peers, a), node: elem(peers, b).node do
      Node.disconnect(node)
    end

    sent
  end

  ## Cluster state

  # Every run starts from fresh trees, with each peer subscribed and registered
  # with every other peer's producer.
  defp start_clean(peers) do
    restart_cluster(peers)

    for peer <- peers do
      remote_run peer, do: EchoPubSub.TestSubscriber.subscribe(PubSubTest, "topic")
    end

    for peer <- peers, producer <- peers, peer != producer do
      assert_registered(peer, producer)
    end
  end

  defp heal(peers) do
    for peer <- peers do
      resume_worker(peer)

      remote_run peer, nodes: Enum.map(peers, & &1.node) do
        Application.put_env(:echo_pubsub, :fault_injection, :ok)
        Enum.each(nodes, &Node.connect/1)
      end
    end
  end

  # A suspended worker resumes; resuming a running one is harmless.
  defp resume_worker(peer) do
    remote_run peer do
      if pid = Process.whereis(PubSubTest.Adapter.Worker), do: :sys.resume(pid)
    end
  end

  ## Checks

  defp assert_delivered(peer, run_id, sent) do
    messages = remote_run(peer, do: EchoPubSub.TestSubscriber.messages())

    refute Enum.any?(messages, &match?({:cursor_expired, _}, &1)),
           "#{peer.node} got cursor_expired: #{inspect(messages)}"

    received = max_seq_without_gaps(peer, messages, run_id)

    assert received == sent,
           "#{peer.node} received up to #{inspect(received)}, expected #{inspect(sent)}"
  end

  # Walks the messages in arrival order and returns the highest seq seen from
  # each producer, failing on the first one that skips past a missing seq.
  defp max_seq_without_gaps(peer, messages, run_id) do
    Enum.reduce(messages, %{}, fn
      {:prop, ^run_id, index, seq}, seen ->
        highest = Map.get(seen, index, 0)

        assert seq <= highest + 1,
               "#{peer.node} got seq #{seq} from producer #{index} after only #{highest}"

        Map.put(seen, index, max(seq, highest))

      _other, seen ->
        seen
    end)
  end
end
