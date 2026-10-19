defmodule NodeApp.MixProject do
  use Mix.Project

  def project do
    [
      app: :node_app,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      releases: [node_app: [include_executables_for: [:unix]]]
    ]
  end

  def application do
    [extra_applications: [:logger], mod: {NodeApp.Application, []}]
  end

  defp deps do
    [
      {:echo_pubsub, path: "../.."},
      {:bandit, "~> 1.5"},
      {:plug, "~> 1.16"}
    ]
  end
end
