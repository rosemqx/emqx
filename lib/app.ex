defmodule Emqx.App do
    use Application
    use Supervisor
    require Logger

    def print_banner(), do: :io.format("Starting ~s on node ~s~n", [:emqx, node()])
    def print_vsn() do 
        {:ok, descr} = :application.get_key(:description)
        {:ok, vsn} = :application.get_key(:vsn)
        :io.format("~s ~s is running now!~n", [descr, vsn])
    end

    defp child(module), do: %{:id       => module,
                              :start    => {module, :start_link, []},
                              :restart  => :permanent,
                              :shutdown => :infinity,
                              :type     => :supervisor,
                              :modules  => [module] }

    defp prepend(xs,true,it), do: xs++[it]
    defp prepend(xs,_,_), do: xs

    def spec(), do: [
        child(:emqx_kernel_sup),
        child(:emqx_sys_sup),
        child(:emqx_mod_sup),
        child(:xio_http_sup)
        ]
        |> prepend(:emqx_boot.is_enabled(:router), child(:emqx_router_sup))
        |> prepend(:emqx_boot.is_enabled(:broker), child(:emqx_broker_sup))
        |> prepend(:emqx_boot.is_enabled(:broker), child(:emqx_cm_sup))

    @impl true
    def init([]) do
      Supervisor.init(spec(), strategy: :one_for_all, max_restart: 0, max_seconds: 1)
    end


    @impl true
    def start(_,arg) do
        :logger.add_handlers(:emqx)
        print_banner()
        :ok = :ekka.start()
        {:ok, sup} = Supervisor.start_link(__MODULE__, arg, name: __MODULE__)

        :ok = :emqx_modules.load()
        :ok = :emqx_plugins.init()
        :emqx_plugins.load()

        if :emqx_boot.is_enabled(:listeners), do: :ok = :emqx_listeners.start()
        :ekka.callback(:prepare, &:emqx.shutdown/1)
        :ekka.callback(:reboot,  &:emqx.reboot/0)
        :ekka.autocluster(:emqx)
        :erlang.register(:emqx, self())

        print_vsn()

        :emqx_mgmt_auth.add_default_app()
        :emqx_mgmt_cli.load()

        {:ok, sup}
    end

    @impl true
    def stop(_) do
        :ekka_mnesia.ensure_stopped()
        :emqx_boot.is_enabled(:listeners) and :emqx_listeners.stop()
        :emqx_modules.unload()
    end

end
