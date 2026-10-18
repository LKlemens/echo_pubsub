defmodule EchoPubSub.Options do
  @moduledoc """
  Resolves the adapter options: each comes from the supervisor opts, else the
  `config :echo_pubsub, <pubsub name>` entry, else its default, and is validated
  so a bad value fails at startup instead of far from its cause.
  """

  @type t :: %{
          pool_size: pos_integer(),
          buffer_size: pos_integer(),
          batch_interval: non_neg_integer(),
          call_timeout: pos_integer(),
          retry_interval: pos_integer(),
          capacity_warning_threshold: number(),
          capacity_warning_interval: non_neg_integer()
        }

  # {option, default, rule}
  @options [
    {:pool_size, 1, :pos_integer},
    {:buffer_size, 10_000, :pos_integer},
    {:batch_interval, 200, :non_neg_integer},
    {:call_timeout, 5000, :pos_integer},
    {:retry_interval, 200, :pos_integer},
    {:capacity_warning_threshold, 0.4, :non_neg_number},
    {:capacity_warning_interval, 60, :non_neg_integer}
  ]

  @doc """
  Resolves every option from `opts`, then `config`, then its default. Raises
  `ArgumentError` naming the option and value when one is invalid.

      iex> EchoPubSub.Options.resolve!([buffer_size: 100], pool_size: 2).pool_size
      2
  """
  @spec resolve!(keyword(), keyword()) :: t()
  def resolve!(opts, config) do
    Map.new(@options, fn {option, default, rule} ->
      value = Keyword.get(opts, option) || Keyword.get(config, option, default)
      validate!(option, value, rule)
      {option, value}
    end)
  end

  defp validate!(option, value, rule) do
    if valid?(rule, value) do
      :ok
    else
      raise ArgumentError,
            "expected #{inspect(option)} to be #{describe(rule)}, got: #{inspect(value)}"
    end
  end

  defp valid?(:pos_integer, value), do: is_integer(value) and value > 0
  defp valid?(:non_neg_integer, value), do: is_integer(value) and value >= 0
  defp valid?(:non_neg_number, value), do: is_number(value) and value >= 0

  defp describe(:pos_integer), do: "a positive integer"
  defp describe(:non_neg_integer), do: "a non-negative integer"
  defp describe(:non_neg_number), do: "a non-negative number"
end
