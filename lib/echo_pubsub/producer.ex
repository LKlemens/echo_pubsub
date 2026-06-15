defmodule EchoPubSub.Producer do
  @moduledoc false
  use GenServer
  require Logger

  def start_link(
        {buffer_size, batch_interval, call_timeout, capacity_warning_threshold,
         capacity_warning_interval, group}
      ) do
    GenServer.start_link(
      __MODULE__,
      {buffer_size, batch_interval, call_timeout, capacity_warning_threshold,
       capacity_warning_interval, group},
      name: name(group)
    )
  end

  def buffer_and_send(group, message) do
    GenServer.call(name(group), {:write, message})
  end

  def name(group) do
    Module.concat(group, Producer)
  end

  defp pg_members(group) do
    :pg.get_members(Phoenix.PubSub, group)
  end

  @impl GenServer
  def init(
        {buffer_size, batch_interval, call_timeout, capacity_warning_threshold,
         capacity_warning_interval, group}
      ) do
    {_ref, pids} = :pg.monitor(Phoenix.PubSub, group)

    Enum.each(pids, &GenServer.call(&1, {:register, node()}))

    state = %{
      group: group,
      write_cursor: 0,
      read_cursors: Map.new(pids, &{node(&1), 0}),
      buffer: :array.new(buffer_size),
      batch_interval: batch_interval,
      call_timeout: call_timeout,
      capacity_warning_threshold: capacity_warning_threshold,
      capacity_warning_interval: capacity_warning_interval,
      flush_timer: nil,
      last_capacity_warning_at: nil
    }

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:write, message}, _from, state) do
    i = rem(state.write_cursor, :array.size(state.buffer))
    buffer = :array.set(i, message, state.buffer)

    flush_timer = maybe_start_flush_timer(state)

    state = %{
      state
      | buffer: buffer,
        write_cursor: state.write_cursor + 1,
        flush_timer: flush_timer
    }

    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_info({_ref, :join, _group, new_pids}, state) do
    {state, has_failure} =
      Enum.reduce(new_pids, {state, false}, fn pid, {acc_state, acc_failure} ->
        {new_state, status} = process_joined(pid, acc_state)
        {new_state, acc_failure or status == :error}
      end)

    state = if has_failure, do: schedule_retry_flush(state), else: state
    {:noreply, state}
  end

  @impl GenServer
  def handle_info({_ref, :leave, _group, _leaving}, state), do: {:noreply, state}

  @impl GenServer
  def handle_info(:flush_all, state) do
    min_read_cursor =
      state.read_cursors |> Map.values() |> Enum.min(fn -> state.write_cursor end)

    buffer_size = state.write_cursor - min_read_cursor
    buffer_capacity = :array.size(state.buffer)

    :telemetry.execute(
      [:echo_pubsub, :buffer, :flush],
      %{buffer_size: buffer_size, buffer_capacity: buffer_capacity},
      %{group: state.group}
    )

    state = maybe_warn_capacity(state, buffer_size, buffer_capacity)

    # Phoenix PubSub handles local dispatch automatically via dispatch/5,
    # so we only send messages to remote nodes to avoid duplicate local delivery
    remote_pids =
      pg_members(state.group)
      |> Enum.filter(&(node(&1) != node()))

    {state, has_failure} =
      Enum.reduce(remote_pids, {state, false}, fn pid, {acc_state, acc_failure} ->
        cursor = Map.get(acc_state.read_cursors, node(pid), 0)
        {new_state, status} = send_messages(pid, cursor, acc_state)
        {new_state, acc_failure or status == :error}
      end)

    # Advance local node cursor since local delivery is handled by Phoenix.PubSub dispatch
    state = %{state | read_cursors: Map.put(state.read_cursors, node(), state.write_cursor)}

    state = %{state | flush_timer: nil}
    state = if has_failure, do: schedule_retry_flush(state), else: state
    {:noreply, state}
  end

  defp process_joined(pid, state) do
    node = node(pid)

    if Map.has_key?(state.read_cursors, node) do
      resume(pid, node, state)
    else
      GenServer.call(pid, {:register, node()})
      {%{state | read_cursors: Map.put(state.read_cursors, node, state.write_cursor)}, :ok}
    end
  end

  defp resume(pid, node, state) do
    oldest = max(state.write_cursor - :array.size(state.buffer), 0)

    case Map.get(state.read_cursors, node, 0) do
      cursor when cursor == state.write_cursor ->
        {state, :ok}

      cursor when cursor < oldest ->
        :telemetry.execute(
          [:echo_pubsub, :buffer, :expired],
          %{count: 1, missed_messages: oldest - cursor},
          %{group: state.group, node: node}
        )

        {send_expired(pid, state), :ok}

      cursor ->
        send_messages(pid, cursor, state)
    end
  end

  defp send_messages(pid, cursor, state) do
    %{read_cursors: read_cursors, write_cursor: write_cursor} = state

    messages =
      cursor..(write_cursor - 1)
      |> Enum.reduce([], fn cursor, acc -> [get_message(cursor, state) | acc] end)
      |> Enum.reverse()

    case safe_call(pid, messages, state.call_timeout, state) do
      :ok ->
        {%{state | read_cursors: Map.put(read_cursors, node(pid), write_cursor)}, :ok}

      :error ->
        emit_sync_failure(state.group, node(pid), length(messages))
        {state, :error}
    end
  end

  defp send_expired(pid, state) do
    GenServer.call(pid, {:expired, node()})
    Map.update!(state, :read_cursors, &Map.delete(&1, node(pid)))
  end

  defp get_message(cursor, state) do
    i = rem(cursor, :array.size(state.buffer))
    :array.get(i, state.buffer)
  end

  defp maybe_start_flush_timer(%{batch_interval: 0} = state) do
    send(self(), :flush_all)
    state
  end

  defp maybe_start_flush_timer(state) do
    if is_nil(state.flush_timer) do
      %{state | flush_timer: Process.send_after(self(), :flush_all, state.batch_interval)}
    else
      state
    end
  end

  @retry_interval 200
  defp schedule_retry_flush(state) do
    if is_nil(state.flush_timer) do
      :telemetry.execute(
        [:echo_pubsub, :retry, :scheduled],
        %{count: 1},
        %{group: state.group}
      )

      %{state | flush_timer: Process.send_after(self(), :flush_all, @retry_interval)}
    else
      state
    end
  end

  defp safe_call(pid, messages, call_timeout, _state) do
    case GenServer.call(pid, messages, call_timeout) do
      :ok -> :ok
      _ -> :error
    end
  catch
    _, _ -> :error
  end

  defp emit_sync_failure(group, node, batch_size) do
    :telemetry.execute(
      [:echo_pubsub, :sync, :failure],
      %{count: 1, batch_size: batch_size},
      %{group: group, node: node}
    )
  end

  defp maybe_warn_capacity(state, buffer_size, buffer_capacity) do
    now = System.monotonic_time(:second)
    ratio = buffer_size / buffer_capacity

    should_warn =
      ratio >= state.capacity_warning_threshold and
        (is_nil(state.last_capacity_warning_at) or
           now - state.last_capacity_warning_at >= state.capacity_warning_interval)

    if should_warn do
      percentage = Float.round(ratio * 100, 1)

      :telemetry.execute(
        [:echo_pubsub, :buffer, :capacity_warning],
        %{buffer_size: buffer_size, buffer_capacity: buffer_capacity, ratio: ratio},
        %{group: state.group}
      )

      Logger.warning(
        "Buffer at #{percentage}% capacity (#{buffer_size}/#{buffer_capacity}) for group #{inspect(state.group)}"
      )

      %{state | last_capacity_warning_at: now}
    else
      state
    end
  end
end
