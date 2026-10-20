defmodule EchoPubSub.BatchLimitTest do
  use ExUnit.Case, async: true

  alias EchoPubSub.Producer.RingBuffer
  alias EchoPubSub.Producer.State

  @node :peer@host

  describe "RingBuffer.pending/4" do
    setup do
      buffer = Enum.reduce(0..9, RingBuffer.new(10), &RingBuffer.put(&2, &1, &1))
      %{buffer: buffer}
    end

    test "returns at most `limit` messages from the read cursor", %{buffer: buffer} do
      assert {:messages, [3, 4, 5]} = RingBuffer.pending(buffer, 3, 10, 3)
    end

    test "returns everything up to the write cursor when the limit is larger", %{buffer: buffer} do
      assert {:messages, [7, 8, 9]} = RingBuffer.pending(buffer, 7, 10, 50)
    end

    test "an infinite limit returns everything", %{buffer: buffer} do
      assert {:messages, messages} = RingBuffer.pending(buffer, 0, 10, :infinity)
      assert messages == Enum.to_list(0..9)
      assert RingBuffer.pending(buffer, 0, 10) == RingBuffer.pending(buffer, 0, 10, :infinity)
    end

    test "a limit of 1 returns one message", %{buffer: buffer} do
      assert {:messages, [0]} = RingBuffer.pending(buffer, 0, 10, 1)
    end

    test "a caught up node stays caught up whatever the limit", %{buffer: buffer} do
      assert RingBuffer.pending(buffer, 10, 10, 3) == :caught_up
    end

    test "expiry does not depend on the limit", %{buffer: buffer} do
      buffer = Enum.reduce(10..14, buffer, &RingBuffer.put(&2, &1, &1))

      assert RingBuffer.pending(buffer, 0, 15, 2) == {:expired, 5}
      assert RingBuffer.pending(buffer, 0, 15) == {:expired, 5}
    end

    test "the limit reads across the wrap of the buffer", %{buffer: buffer} do
      buffer = Enum.reduce(10..14, buffer, &RingBuffer.put(&2, &1, &1))

      assert {:messages, [8, 9, 10, 11]} = RingBuffer.pending(buffer, 8, 15, 4)
    end
  end

  describe "batch limits" do
    test "a node without a limit gets the configured max" do
      assert State.batch_limit(state(:infinity), @node) == :infinity
      assert State.batch_limit(state(100), @node) == 100
    end

    test "a failed batch halves the limit, never below 1" do
      state = State.shrink_batch_limit(state(:infinity), @node, 40)
      assert State.batch_limit(state, @node) == 20

      state = State.shrink_batch_limit(state, @node, 20)
      assert State.batch_limit(state, @node) == 10

      state = State.shrink_batch_limit(state, @node, 1)
      assert State.batch_limit(state, @node) == 1
    end

    test "growing doubles the limit" do
      state =
        state(:infinity) |> State.shrink_batch_limit(@node, 40) |> State.grow_batch_limit(@node)

      assert State.batch_limit(state, @node) == 40
    end

    test "growing is capped at the configured max, which clears the limit" do
      state = state(30) |> State.shrink_batch_limit(@node, 40)
      assert State.batch_limit(state, @node) == 20

      state = State.grow_batch_limit(state, @node)
      assert State.batch_limit(state, @node) == 30
      refute Map.has_key?(state.batch_limits, @node)
    end

    test "growing a node with no limit changes nothing" do
      state = state(30)
      assert State.grow_batch_limit(state, @node) == state
    end

    test "resetting and forgetting drop the limit" do
      shrunk = State.shrink_batch_limit(state(:infinity), @node, 40)

      assert State.batch_limit(State.reset_batch_limit(shrunk, @node), @node) == :infinity
      assert State.batch_limit(State.forget(shrunk, [@node]), @node) == :infinity
    end

    test "pending applies the node's limit" do
      state =
        Enum.reduce(0..9, state(:infinity), &State.write(&2, &1))
        |> State.registered(@node, self(), 2)
        |> State.shrink_batch_limit(@node, 6)

      assert {:messages, [2, 3, 4]} = State.pending(state, @node)
    end
  end

  defp state(max_batch_size) do
    State.new(:batch_limit_test,
      buffer_size: 10,
      max_batch_size: max_batch_size,
      batch_interval: 0,
      call_timeout: 1000,
      retry_interval: 50,
      capacity_warning_threshold: 2.0,
      capacity_warning_interval: 60
    )
  end
end
