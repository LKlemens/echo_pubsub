defmodule NodeApp.Router do
  @moduledoc """
  HTTP API for the Jepsen client.

    * `POST /broadcast?id=N` broadcasts `{:jepsen, node, boot, N}` and replies
      `{"node": ..., "boot": ...}` once the producer has buffered it.
    * `GET /log` returns the subscriber log as JSON lines.
  """
  use Plug.Router

  plug(:match)
  plug(:fetch_query_params)
  plug(:dispatch)

  post "/broadcast" do
    id = String.to_integer(conn.query_params["id"])
    boot = NodeApp.Application.boot()

    case broadcast({:jepsen, Atom.to_string(node()), boot, id}) do
      :ok -> send_json(conn, 200, %{node: node(), boot: boot})
      {:error, reason} -> send_json(conn, 503, %{error: inspect(reason)})
    end
  end

  get "/log" do
    log = NodeApp.Subscriber.read(Application.fetch_env!(:node_app, :log_path))
    conn |> put_resp_content_type("application/x-ndjson") |> send_resp(200, log)
  end

  match _ do
    send_resp(conn, 404, "not found")
  end

  # A producer call that times out may still buffer the message, so the client
  # must treat a 503 as indeterminate.
  defp broadcast(message) do
    Phoenix.PubSub.broadcast(NodeApp.PubSub, NodeApp.Subscriber.topic(), message)
  catch
    :exit, reason -> {:error, reason}
  end

  defp send_json(conn, status, body) do
    conn |> put_resp_content_type("application/json") |> send_resp(status, JSON.encode!(body))
  end
end
