defmodule NodeApp.Subscriber do
  @moduledoc """
  Records every message on the `jepsen` topic as a JSON line in a log file.

  The file is opened raw, so each line reaches the OS on write and survives a
  SIGKILL of the VM. The first line of every boot is a `boot` entry, which tells
  the checker the node restarted and may have lost its mailbox.

  Every entry carries `ts`, the wall clock in milliseconds. The Jepsen nodes
  share one host clock, so the checker can compare it with when a broadcast was
  invoked and excuse a loss only by a restart or expiry that came after it.
  """
  use GenServer

  @topic "jepsen"

  @doc "The topic Jepsen broadcasts on."
  @spec topic() :: String.t()
  def topic, do: @topic

  @doc "Starts the subscriber, appending to the log at `path`."
  @spec start_link(Path.t()) :: GenServer.on_start()
  def start_link(path), do: GenServer.start_link(__MODULE__, path, name: __MODULE__)

  @doc """
  Reads the whole log at `path`, oldest line first.

      NodeApp.Subscriber.read("deliveries.log")
      #=> "{\\"type\\":\\"boot\\",...}\\n..."
  """
  @spec read(Path.t()) :: binary()
  def read(path) do
    case File.read(path) do
      {:ok, contents} -> contents
      {:error, :enoent} -> ""
    end
  end

  @impl GenServer
  def init(path) do
    File.mkdir_p!(Path.dirname(path))
    {:ok, file} = :file.open(path, [:append, :raw, :binary])
    :ok = Phoenix.PubSub.subscribe(NodeApp.PubSub, @topic)
    write(file, %{type: "boot", boot: NodeApp.Application.boot()})
    {:ok, file}
  end

  @impl GenServer
  def handle_info({:jepsen, from, boot, id}, file) do
    write(file, %{type: "msg", from: from, boot: boot, id: id})
    {:noreply, file}
  end

  def handle_info({:cursor_expired, from}, file) do
    write(file, %{type: "expired", from: from})
    {:noreply, file}
  end

  defp write(file, entry) do
    entry = Map.put(entry, :ts, System.os_time(:millisecond))
    :ok = :file.write(file, [JSON.encode!(entry), ?\n])
  end
end
