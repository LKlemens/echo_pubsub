defmodule EchoPubSub.MixProject do
  use Mix.Project

  @source_url "https://github.com/LKlemens/echo_pubsub"
  @version "0.2.1"

  def project do
    [
      app: :echo_pubsub,
      version: @version,
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      test_coverage: [tool: ExCoveralls],
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
        extras: [
          "README.md",
          "docs/how-it-works.md",
          "CHANGELOG.md",
          "LICENSE"
        ],
        source_ref: "v#{@version}"
      ],
      package: [
        maintainers: ["Eric Newbury"],
        licenses: ["MIT"],
        links: %{"GitHub" => @source_url}
      ]
    ]
  end

  def cli do
    [preferred_envs: [coveralls: :test, "coveralls.html": :test, "coveralls.github": :test]]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger] ++ test_applications(Mix.env())
    ]
  end

  # :tools provides :cover, which the test cluster uses in coverage runs.
  defp test_applications(:test), do: [:tools]
  defp test_applications(_), do: []

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:phoenix_pubsub, "~> 2.0"},
      {:telemetry, "~> 1.0"},
      {:typed_struct, "~> 0.3", runtime: false},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:excoveralls, "~> 0.18", only: :test},
      {:stream_data, "~> 1.1", only: :test}
    ]
  end
end
