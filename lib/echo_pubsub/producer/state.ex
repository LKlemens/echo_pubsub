defmodule EchoPubSub.Producer.State do
  @moduledoc """
  The producer's state and the only place that changes it. Other modules read
  fields directly but update them through the named functions here.
  """
  use TypedStruct

  alias EchoPubSub.Producer.RingBuffer

  @type cursor :: non_neg_integer()

  @typedoc """
  Fields:

    * `group` - the `:pg` group this producer delivers to (its delivery scope)
    * `write_cursor` - count of messages ever written; the next write lands here
    * `read_cursors` - map of node to its next-needed cursor; everything below it that node has acked.
      A node is absent until its registration is acked
    * `registering` - map of node to the `{task reference, start cursor, worker pid}` of its pending
      registration; the reference is `nil` while a failed attempt waits for its retry
    * `workers` - map of node to the worker pid this producer last registered with
    * `in_flight` - map of node to the `{task reference, target cursor, truncated?}` of the batch being
      delivered to it, or `{task reference, :probe, false}` while it is being probed. `truncated?` is true
      when the batch stopped short of the write cursor at the time it was prepared
    * `batch_limits` - map of node to the batch size limit it is held to after a failed delivery;
      absent means the configured `max_batch_size`
    * `suspects` - set (map to `true`) of nodes whose last delivery failed; they are probed before the next batch
    * `buffer` - ring buffer of the last `buffer_size` messages; oldest overwritten on wrap
    * `max_batch_size` - most messages sent to a node in one call, or `:infinity`
    * `batch_interval` - milliseconds to batch writes before a flush; `0` flushes immediately
    * `call_timeout` - timeout in milliseconds for synchronous calls to remote nodes
    * `retry_interval` - milliseconds to wait before flushing again after a failure
    * `capacity_warning_threshold` - buffer fill ratio that triggers a capacity warning
    * `capacity_warning_interval` - minimum seconds between capacity warnings
    * `flush_timer` - reference of the pending flush timer, or `nil` when none is scheduled
    * `last_capacity_warning_at` - monotonic seconds of the last capacity warning, or `nil` if never
  """
  typedstruct enforce: true do
    field(:group, atom())
    field(:write_cursor, cursor(), default: 0)
    field(:read_cursors, %{node() => cursor()}, default: %{})
    field(:registering, %{node() => {reference() | nil, cursor(), pid()}}, default: %{})
    field(:workers, %{node() => pid()}, default: %{})
    field(:in_flight, %{node() => {reference(), cursor() | :probe, boolean()}}, default: %{})
    field(:batch_limits, %{node() => pos_integer()}, default: %{})
    field(:suspects, %{node() => true}, default: %{})
    field(:buffer, RingBuffer.t())
    field(:max_batch_size, pos_integer() | :infinity)
    field(:batch_interval, timeout())
    field(:call_timeout, timeout())
    field(:retry_interval, pos_integer())
    field(:capacity_warning_threshold, number())
    field(:capacity_warning_interval, non_neg_integer())
    field(:flush_timer, reference() | nil, enforce: false)
    field(:last_capacity_warning_at, integer() | nil, enforce: false)
  end

  @doc "A fresh state for `group`: empty buffer, no known nodes."
  @spec new(atom(), [EchoPubSub.Producer.option()]) :: t()
  def new(group, opts) do
    %__MODULE__{
      group: group,
      buffer: RingBuffer.new(Keyword.fetch!(opts, :buffer_size)),
      max_batch_size: Keyword.fetch!(opts, :max_batch_size),
      batch_interval: Keyword.fetch!(opts, :batch_interval),
      call_timeout: Keyword.fetch!(opts, :call_timeout),
      retry_interval: Keyword.fetch!(opts, :retry_interval),
      capacity_warning_threshold: Keyword.fetch!(opts, :capacity_warning_threshold),
      capacity_warning_interval: Keyword.fetch!(opts, :capacity_warning_interval)
    }
  end

  @doc "Whether a registration task for `node` is running (not a failed one waiting to retry)."
  @spec registration_running?(t(), node()) :: boolean()
  def registration_running?(state, node) do
    match?({ref, _start, _pid} when is_reference(ref), state.registering[node])
  end

  @doc "The start cursor a failed registration of `node` retries from, or `nil`."
  @spec retry_start(t(), node()) :: cursor() | nil
  def retry_start(state, node) do
    case state.registering[node] do
      {nil, start, _pid} -> start
      _ -> nil
    end
  end

  @doc "What registered `node` still needs from the buffer, at most `batch_limit/2` messages."
  @spec pending(t(), node()) :: RingBuffer.pending()
  def pending(state, node) do
    RingBuffer.pending(
      state.buffer,
      Map.fetch!(state.read_cursors, node),
      state.write_cursor,
      batch_limit(state, node)
    )
  end

  @doc "The most messages the next batch to `node` may hold."
  @spec batch_limit(t(), node()) :: pos_integer() | :infinity
  def batch_limit(state, node), do: Map.get(state.batch_limits, node, state.max_batch_size)

  @doc """
  Holds `node` to half of the `batch_size` that just failed (at least 1), so a
  node too slow for a big batch is sent ones it can finish within `call_timeout`.
  """
  @spec shrink_batch_limit(t(), node(), pos_integer()) :: t()
  def shrink_batch_limit(state, node, batch_size) do
    %{state | batch_limits: Map.put(state.batch_limits, node, max(1, div(batch_size, 2)))}
  end

  @doc """
  Doubles the limit of a held `node`, up to `max_batch_size`; a node not held
  keeps the configured size.
  """
  @spec grow_batch_limit(t(), node()) :: t()
  def grow_batch_limit(state, node) do
    case Map.fetch(state.batch_limits, node) do
      {:ok, limit} when state.max_batch_size == :infinity or limit * 2 < state.max_batch_size ->
        put_batch_limit(state, node, limit * 2)

      {:ok, _limit} ->
        reset_batch_limit(state, node)

      :error ->
        state
    end
  end

  @doc "Sends `node` the configured `max_batch_size` again."
  @spec reset_batch_limit(t(), node()) :: t()
  def reset_batch_limit(state, node),
    do: %{state | batch_limits: Map.delete(state.batch_limits, node)}

  defp put_batch_limit(state, node, limit),
    do: %{state | batch_limits: Map.put(state.batch_limits, node, limit)}

  @doc "Whether `node` has not acked every write yet."
  @spec behind?(t(), node()) :: boolean()
  def behind?(state, node), do: Map.get(state.read_cursors, node, 0) < state.write_cursor

  @doc "Whether `node` is further behind than the buffer can replay."
  @spec beyond_buffer?(t(), node()) :: boolean()
  def beyond_buffer?(state, node) do
    state.write_cursor - state.read_cursors[node] > RingBuffer.capacity(state.buffer)
  end

  @doc "Writes the slowest node has not acked yet."
  @spec unacked(t()) :: non_neg_integer()
  def unacked(state) do
    min_read_cursor = state.read_cursors |> Map.values() |> Enum.min(fn -> state.write_cursor end)
    state.write_cursor - min_read_cursor
  end

  @doc """
  The registration or delivery a task `ref` belongs to. Every ref is in exactly
  one map: registrations and deliveries never share a ref.
  """
  @spec find_task(t(), reference()) ::
          {:registration, node(), cursor(), pid()}
          | {:delivery, node(), cursor() | :probe, boolean()}
          | :unknown
  def find_task(state, ref) do
    case {find_entry(state.registering, ref), find_entry(state.in_flight, ref)} do
      {{node, {_ref, start, pid}}, nil} -> {:registration, node, start, pid}
      {nil, {node, {_ref, target, truncated?}}} -> {:delivery, node, target, truncated?}
      {nil, nil} -> :unknown
    end
  end

  defp find_entry(tasks, ref), do: Enum.find(tasks, fn {_node, task} -> elem(task, 0) == ref end)

  @doc "Stores `message` at the write cursor and moves the cursor on."
  @spec write(t(), term()) :: t()
  def write(state, message) do
    buffer = RingBuffer.put(state.buffer, state.write_cursor, message)
    %{state | buffer: buffer, write_cursor: state.write_cursor + 1}
  end

  @doc "Records a running registration task for `node`."
  @spec track_registration(t(), node(), reference(), cursor(), pid()) :: t()
  def track_registration(state, node, ref, start, pid) do
    %{state | registering: Map.put(state.registering, node, {ref, start, pid})}
  end

  @doc "Keeps a failed registration's start cursor for the retry; no task runs meanwhile."
  @spec registration_failed(t(), node(), cursor(), pid()) :: t()
  def registration_failed(state, node, start, pid) do
    %{state | registering: Map.put(state.registering, node, {nil, start, pid})}
  end

  @doc "Marks `node` registered with `pid`, keeping its cursor if it had one."
  @spec registered(t(), node(), pid(), cursor()) :: t()
  def registered(state, node, pid, start) do
    %{
      state
      | registering: Map.delete(state.registering, node),
        workers: Map.put(state.workers, node, pid),
        read_cursors: Map.put_new(state.read_cursors, node, start)
    }
  end

  @doc """
  Records a batch in flight to `node`, acking up to `target` once it succeeds, or a `:probe`.
  `truncated?` says the batch stopped short of the write cursor, so more is waiting.
  """
  @spec track_delivery(t(), node(), reference(), cursor() | :probe, boolean()) :: t()
  def track_delivery(state, node, ref, target, truncated? \\ false) do
    %{state | in_flight: Map.put(state.in_flight, node, {ref, target, truncated?})}
  end

  @doc "Forgets the batch in flight to `node`."
  @spec untrack_delivery(t(), node()) :: t()
  def untrack_delivery(state, node), do: %{state | in_flight: Map.delete(state.in_flight, node)}

  @doc "Whether `node`'s last delivery failed, so it must answer a probe first."
  @spec suspect?(t(), node()) :: boolean()
  def suspect?(state, node), do: Map.has_key?(state.suspects, node)

  @doc "Marks `node` as suspect after a failed delivery."
  @spec suspect(t(), node()) :: t()
  def suspect(state, node), do: %{state | suspects: Map.put(state.suspects, node, true)}

  @doc "Clears `node` after it answered a probe."
  @spec clear_suspect(t(), node()) :: t()
  def clear_suspect(state, node), do: %{state | suspects: Map.delete(state.suspects, node)}

  @doc "Records that `node` acked every message below `target`."
  @spec ack(t(), node(), cursor()) :: t()
  def ack(state, node, target),
    do: %{state | read_cursors: Map.put(state.read_cursors, node, target)}

  @doc "Acks every write for this node: Phoenix delivered them to local subscribers already."
  @spec ack_local(t()) :: t()
  def ack_local(state), do: ack(state, node(), state.write_cursor)

  @doc "Drops every trace of `nodes`, so they register from scratch if they return."
  @spec forget(t(), [node()]) :: t()
  def forget(state, nodes) do
    %{
      state
      | read_cursors: Map.drop(state.read_cursors, nodes),
        workers: Map.drop(state.workers, nodes),
        registering: Map.drop(state.registering, nodes),
        suspects: Map.drop(state.suspects, nodes),
        batch_limits: Map.drop(state.batch_limits, nodes)
    }
  end

  @doc "Records the pending flush timer."
  @spec set_flush_timer(t(), reference()) :: t()
  def set_flush_timer(state, ref), do: %{state | flush_timer: ref}

  @doc "Clears the flush timer once the flush runs."
  @spec clear_flush_timer(t()) :: t()
  def clear_flush_timer(state), do: %{state | flush_timer: nil}

  @doc "Records when the last capacity warning fired."
  @spec set_last_warning(t(), integer() | nil) :: t()
  def set_last_warning(state, at), do: %{state | last_capacity_warning_at: at}
end
