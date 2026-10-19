defmodule EchoPubSub.Producer.Telemetry do
  @moduledoc "The producer's telemetry events and its throttled buffer-capacity warning."

  require Logger

  @doc "`[:echo_pubsub, :buffer, :flush]`: unacked messages vs. buffer capacity."
  @spec flush(atom(), non_neg_integer(), pos_integer()) :: :ok
  def flush(group, buffer_size, buffer_capacity) do
    :telemetry.execute(
      [:echo_pubsub, :buffer, :flush],
      %{buffer_size: buffer_size, buffer_capacity: buffer_capacity},
      %{group: group}
    )
  end

  @doc "`[:echo_pubsub, :buffer, :expired]`: `node` was told it missed `missed` messages."
  @spec expired(atom(), node(), pos_integer()) :: :ok
  def expired(group, node, missed) do
    :telemetry.execute(
      [:echo_pubsub, :buffer, :expired],
      %{count: 1, missed_messages: missed},
      %{group: group, node: node}
    )
  end

  @doc "`[:echo_pubsub, :sync, :batch]`: a batch of `batch_size` was sent to `node`."
  @spec sync_batch(atom(), node(), pos_integer()) :: :ok
  def sync_batch(group, node, batch_size) do
    :telemetry.execute(
      [:echo_pubsub, :sync, :batch],
      %{count: 1, batch_size: batch_size},
      %{group: group, node: node}
    )
  end

  @doc "`[:echo_pubsub, :sync, :failure]`: a batch of `batch_size` to `node` failed."
  @spec sync_failure(atom(), node(), pos_integer()) :: :ok
  def sync_failure(group, node, batch_size) do
    :telemetry.execute(
      [:echo_pubsub, :sync, :failure],
      %{count: 1, batch_size: batch_size},
      %{group: group, node: node}
    )
  end

  @doc "`[:echo_pubsub, :retry, :scheduled]`: a retry flush was scheduled."
  @spec retry_scheduled(atom()) :: :ok
  def retry_scheduled(group) do
    :telemetry.execute([:echo_pubsub, :retry, :scheduled], %{count: 1}, %{group: group})
  end

  @doc """
  Logs and emits `[:echo_pubsub, :buffer, :capacity_warning]` when the fill
  ratio reaches `threshold` and `interval` seconds passed since `last_at`.
  Returns the new `last_at`.
  """
  @spec maybe_warn_capacity(
          atom(),
          non_neg_integer(),
          non_neg_integer(),
          number(),
          non_neg_integer(),
          integer() | nil
        ) :: integer() | nil
  def maybe_warn_capacity(group, buffer_size, buffer_capacity, threshold, interval, last_at) do
    now = System.monotonic_time(:second)
    ratio = buffer_size / buffer_capacity

    if ratio >= threshold and (is_nil(last_at) or now - last_at >= interval) do
      :telemetry.execute(
        [:echo_pubsub, :buffer, :capacity_warning],
        %{buffer_size: buffer_size, buffer_capacity: buffer_capacity, ratio: ratio},
        %{group: group}
      )

      Logger.warning(
        "Buffer at #{Float.round(ratio * 100, 1)}% capacity (#{buffer_size}/#{buffer_capacity}) for group #{inspect(group)}"
      )

      now
    else
      last_at
    end
  end
end
