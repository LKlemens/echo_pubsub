defmodule EchoPubSub.DirectBroadcastTest do
  use EchoPubSub.ClusterCase, async: true

  setup_all :start_shared_peers
  setup :reset_peers

  # Like PG2, a direct broadcast is a send: it never blocks or exits the caller,
  # so Phoenix.Tracker and Presence can call it on their hot paths.
  test "returns :ok for a node that does not exist, without exiting", %{peer1: peer1} do
    assert :ok ==
             remote_run(peer1,
               do:
                 Phoenix.PubSub.direct_broadcast(
                   :"nobody@127.0.0.1",
                   PubSubTest,
                   "topic",
                   :message
                 )
             )
  end

  test "returns promptly while the worker is hung, and delivers after it resumes", %{
    peer1: peer1,
    peer2: peer2
  } do
    remote_run peer2, do: :sys.suspend(PubSubTest.Adapter.Worker)

    {elapsed_us, result} =
      :timer.tc(fn ->
        remote_run peer1, node: peer2.node do
          Phoenix.PubSub.direct_broadcast(node, PubSubTest, "topic", :hung)
        end
      end)

    assert result == :ok
    assert elapsed_us < 200_000

    remote_run peer2, do: :sys.resume(PubSubTest.Adapter.Worker)
    assert_peer_receive peer2, :hung
  end
end
