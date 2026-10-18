# Under `mix test --cover`, the runner joins distribution as a hidden node so it
# can collect coverage from the peer nodes the cluster tests start. Hidden, it
# never shows up in the peers' clusters or bridges two of them.
# A running VM does not start epmd itself (fresh CI runners have none), so start it.
if Process.whereis(:cover_server) do
  {_output, 0} = System.cmd("epmd", ["-daemon"])

  {:ok, _} =
    :net_kernel.start(:"test_runner_#{System.pid()}", %{name_domain: :shortnames, hidden: true})
end

ExUnit.start()
