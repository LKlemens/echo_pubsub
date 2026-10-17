defmodule EchoPubSub.CapacityWarningTest do
  use EchoPubSub.ClusterCase, async: true

  test "logs warning when buffer reaches 40% capacity" do
    # buffer_size: 10, so 40% = 4 messages
    [peer1, peer2] = spawn_nodes(["warn_node1", "warn_node2"])

    remote_run peer2 do
      EchoPubSub.TestSubscriber.subscribe(PubSubTest, "topic")
    end

    # Disconnect peer2 so messages accumulate in buffer
    remote_run peer2, node: peer1.node do
      Node.disconnect(node)
    end

    # Capture log on peer1 where the producer runs
    log =
      remote_run peer1 do
        ExUnit.CaptureLog.capture_log(fn ->
          # Send 4 messages to reach 40% capacity (4/10 = 40%)
          for i <- 1..4 do
            Phoenix.PubSub.broadcast!(PubSubTest, "topic", {:msg, i})
          end

          # Force flush to trigger the warning check
          send(PubSubTest.Adapter.Producer, :flush_all)
          Process.sleep(50)
        end)
      end

    assert log =~ "Buffer at 40.0% capacity (4/10)"
    assert log =~ "PubSubTest.Adapter"
  end

  test "does not log warning when buffer is below 40% capacity" do
    [peer1, peer2] = spawn_nodes(["no_warn_node1", "no_warn_node2"])

    remote_run peer2 do
      EchoPubSub.TestSubscriber.subscribe(PubSubTest, "topic")
    end

    remote_run peer2, node: peer1.node do
      Node.disconnect(node)
    end

    log =
      remote_run peer1 do
        ExUnit.CaptureLog.capture_log(fn ->
          # Send only 3 messages (30% < 40%)
          for i <- 1..3 do
            Phoenix.PubSub.broadcast!(PubSubTest, "topic", {:msg, i})
          end

          send(PubSubTest.Adapter.Producer, :flush_all)
          Process.sleep(50)
        end)
      end

    refute log =~ "Buffer at"
  end
end
