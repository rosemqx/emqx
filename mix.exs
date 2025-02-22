defmodule Emqx.MixProject do
  use Mix.Project

  def project do
    [
      app: :emqx,
      version: "4.2.0",
      description: "EMQ X Broker",
      elixir: "~> 1.9",
      start_permanent: Mix.env() == :prod,
      erlc_paths: ["src"],
      elixirc_options: [warnings_as_errors: true],
      deps: deps()
    ]
  end

  def application do
    [
      mod: {Emqx.App, []},
      extra_applications: [:logger, :os_mon],
      applications: [
        :kernel,
        :stdlib,
        :inets,
        :mnesia,
        :asn1,
        :crypto,
        :gproc,
        :public_key,
        :ssl,
        :ranch,
        :esockd,
        :jsone,
        :gen_rpc,
        :ssl_verify_fun,
        :cowlib,
        :cowboy,
        :replayq,
        :ekka,
        :minirest
      ]
    ]
  end

  def deps do
    [
      {:gproc, github: "voxoz/gproc"},
      {:ekka, github: "rosemqx/ekka", ref: "v0.7"},
      {:esockd, github: "rosemqx/esockd", ref: "v5.7.3"},
      {:gen_rpc, github: "rosemqx/gen_rpc"},
      {:replayq, github: "rosemqx/replayq"},
      {:minirest, github: "rosemqx/minirest", ref: "emqx42"},
      {:jsone, "~> 1.5.5", override: true},
      {:cowlib, "~> 2.9.1", override: true},
      {:cowboy, "~> 2.8.0", override: true},
      {:ranch, "~> 1.7.1", override: true}
    ]
  end
end
