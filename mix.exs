defmodule EchoPubSub.MixProject do
  use Mix.Project

  @source_url "https://github.com/LKlemens/echo_pubsub"
  @version "0.1.1"

  def project do
    [
      app: :echo_pubsub,
      version: @version,
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      dialyzer: [
        plt_local_path: "priv/plts",
        plt_core_path: "priv/plts",
        plt_add_apps: [:ex_unit, :mix]
      ],
      # Docs
      name: "EchoPubSub",
      description: "A :pg based Phoenix PubSub adapter with at-least-once delivery",
      source_url: @source_url,
      docs: [
        # The main page in the docs
        main: "EchoPubSub",
        extras: ["README.md", "CHANGELOG.md", "LICENSE"],
        source_ref: "v#{@version}"
      ],
      package: [
        maintainers: ["Eric Newbury"],
        licenses: ["MIT"],
        links: %{"GitHub" => @source_url}
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:phoenix_pubsub, "~> 2.0"},
      {:telemetry, "~> 1.0"},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end
end
