-module(xio_http_sup).
-description("Manage ERP.UNO MQ http listeners").
-behaviour(supervisor).
-export([init/1, start_link/0]).

start_link() -> supervisor:start_link({local,?MODULE}, ?MODULE, []).

init([]) ->
    Admin = #{ id => emqx_admin
             , start => {emqx_admin, start_link, []}
             , restart => permanent
             , shutdown => 15000
             , type => worker
             , modules => [emqx_admin]
             },
    Dispatch = cowboy_router:compile([{'_', [
        {"/status", emqx_mgmt_http, []}] ++
        minirest:handlers([{"/api/v4/[...]", minirest, http_handlers()}])
    }]),

    Opts = #{
      connection_type => worker,
      handshake_timeout => 10000,
      max_connections => 1000,
      num_acceptors => 100,
      shutdown => 5000,
      socket_opts => [{port, application:get_env(emqx, mgmt_port, 8080)}]
    },

    Spec = ranch:child_spec('http:management', ranch_tcp, Opts, cowboy_clear, #{env => #{dispatch => Dispatch}}),

    {ok, {#{strategy => one_for_one, intensity => 1, period => 5}, [Admin, Spec]}}.

http_handlers() ->
    Cfg = #{apps => [emqx], modules=> []},
    Hnd = minirest_handler:init(Cfg),
    [{"/api/v4", Hnd, [{authorization, fun emqx_mgmt_http:is_authorized/1}]}].
