defmodule NodeApp.RouterTest do
  use ExUnit.Case, async: false
  import Plug.Test

  @log_path Application.compile_env!(:node_app, :log_path)

  test "a broadcast lands in the log, tagged with this node and boot" do
    id = System.unique_integer([:positive])
    conn = NodeApp.Router.call(conn(:post, "/broadcast?id=#{id}"), [])

    assert conn.status == 200
    assert %{"boot" => boot} = JSON.decode!(conn.resp_body)
    assert boot == NodeApp.Application.boot()

    entry = %{"type" => "msg", "from" => "nonode@nohost", "boot" => boot, "id" => id}
    assert_eventually(fn -> Enum.any?(log_entries(), &(Map.delete(&1, "ts") == entry)) end)
    assert log_entries() |> Enum.find(&(&1["id"] == id)) |> Map.fetch!("ts") |> is_integer()
  end

  test "the log survives a subscriber restart, which adds a boot entry" do
    id = System.unique_integer([:positive])
    NodeApp.Router.call(conn(:post, "/broadcast?id=#{id}"), [])
    assert_eventually(fn -> Enum.any?(log_entries(), &(&1["id"] == id)) end)
    boots_before = Enum.count(log_entries(), &(&1["type"] == "boot"))

    pid = Process.whereis(NodeApp.Subscriber)
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, _, _, _}
    assert_eventually(fn -> Process.whereis(NodeApp.Subscriber) not in [nil, pid] end)

    entries = log_entries()
    assert Enum.any?(entries, &(&1["id"] == id))
    assert Enum.count(entries, &(&1["type"] == "boot")) == boots_before + 1
  end

  test "GET /log returns the log as JSON lines" do
    conn = NodeApp.Router.call(conn(:get, "/log"), [])

    assert conn.status == 200

    assert conn.resp_body
           |> String.split("\n", trim: true)
           |> Enum.all?(&match?(%{}, JSON.decode!(&1)))
  end

  defp log_entries do
    @log_path
    |> NodeApp.Subscriber.read()
    |> String.split("\n", trim: true)
    |> Enum.map(&JSON.decode!/1)
  end

  defp assert_eventually(fun, attempts \\ 50) do
    cond do
      fun.() ->
        :ok

      attempts == 0 ->
        flunk("condition never became true")

      true ->
        Process.sleep(20)
        assert_eventually(fun, attempts - 1)
    end
  end
end
