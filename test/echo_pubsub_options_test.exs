defmodule EchoPubSubOptionsTest do
  use ExUnit.Case, async: true

  doctest EchoPubSub.Options

  @required [name: OptionsTest, adapter_name: OptionsTest.Adapter]

  # Validation runs before init starts anything, so rejected cases need no cleanup.
  for {option, value} <- [
        pool_size: 0,
        pool_size: 1.5,
        buffer_size: 0,
        buffer_size: -10,
        call_timeout: 0,
        call_timeout: :infinity,
        batch_interval: -1,
        batch_interval: "200",
        capacity_warning_interval: -1,
        capacity_warning_threshold: -0.1,
        capacity_warning_threshold: "0.4"
      ] do
    test "rejects #{option}: #{inspect(value)}" do
      error =
        assert_raise ArgumentError, fn ->
          EchoPubSub.init([{unquote(option), unquote(Macro.escape(value))} | @required])
        end

      assert error.message =~ inspect(unquote(option))
      assert error.message =~ inspect(unquote(Macro.escape(value)))
    end
  end

  test "rejects an invalid value from the application config" do
    # 1. The bad value comes from config, not from the supervisor opts.
    Application.put_env(:echo_pubsub, ConfigOptionsTest, buffer_size: 0)
    on_exit(fn -> Application.delete_env(:echo_pubsub, ConfigOptionsTest) end)

    # 2. It is still rejected.
    assert_raise ArgumentError, ~r/:buffer_size/, fn ->
      EchoPubSub.init(name: ConfigOptionsTest, adapter_name: ConfigOptionsTest.Adapter)
    end
  end

  test "accepts the smallest valid values" do
    # 1. Edge values: immediate flush, warn at any fill, no warning throttle.
    opts = [
      name: EdgeOptionsTest,
      adapter_name: EdgeOptionsTest.Adapter,
      pool_size: 1,
      buffer_size: 1,
      call_timeout: 1,
      batch_interval: 0,
      capacity_warning_interval: 0,
      capacity_warning_threshold: 0
    ]

    # 2. init returns the child specs without starting them.
    assert {:ok, _spec} = EchoPubSub.init(opts)
  end

  test "accepts a threshold above 1, which never warns" do
    opts = [
      name: HighThresholdTest,
      adapter_name: HighThresholdTest.Adapter,
      capacity_warning_threshold: 2.0
    ]

    assert {:ok, _spec} = EchoPubSub.init(opts)
  end
end
