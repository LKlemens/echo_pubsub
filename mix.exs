defmodule EchoPubSub.MixProject do
  use Mix.Project

  @source_url "https://github.com/LKlemens/echo_pubsub"
  @version "0.1.4"

  def project do
    [
      app: :echo_pubsub,
      version: @version,
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      releases: [echo_pubsub: [include_executables_for: [:unix]]],
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
          "docs/benchmarks.md",
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

  defp elixirc_paths(:test), do: ["lib", "test/support", "bench/fly"]
  defp elixirc_paths(:bench), do: ["lib", "bench/fly"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help compile.app" to learn about applications.
  # The :bench release (Fly cluster) starts the bench app; the published library
  # never does — it stays a plain adapter.
  def application do
    [extra_applications: [:logger]] ++ bench_mod(Mix.env())
  end

  defp bench_mod(:bench), do: [mod: {EchoPubSub.Bench.App, []}]
  defp bench_mod(_), do: []

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:phoenix_pubsub, "~> 2.0"},
      {:telemetry, "~> 1.0"},
      {:typed_struct, "~> 0.3", runtime: false},
      {:libcluster, "~> 3.3", only: [:bench]},
      {:delta_crdt, "~> 0.6", only: [:dev, :test]},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end
end
