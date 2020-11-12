%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_mgmt_api_clients).

-include("emqx_mgmt.hrl").
-include("emqx_mqtt.hrl").
-include("emqx.hrl").

-import(minirest, [ return/0
                  , return/1
                  ]).

-define(CLIENT_QS_SCHEMA, {emqx_channel_info,
        [{<<"clientid">>, binary},
         {<<"username">>, binary},
         {<<"zone">>, atom},
         {<<"ip_address">>, ip},
         {<<"conn_state">>, atom},
         {<<"clean_start">>, atom},
         {<<"proto_name">>, binary},
         {<<"proto_ver">>, integer},
         {<<"_like_clientid">>, binary},
         {<<"_like_username">>, binary},
         {<<"_gte_created_at">>, timestamp},
         {<<"_lte_created_at">>, timestamp},
         {<<"_gte_connected_at">>, timestamp},
         {<<"_lte_connected_at">>, timestamp}]}).

-rest_api(#{name   => list_clients,
            method => 'GET',
            path   => "/clients/",
            func   => list,
            descr  => "A list of clients on current node"}).

-rest_api(#{name   => list_node_clients,
            method => 'GET',
            path   => "nodes/:atom:node/clients/",
            func   => list,
            descr  => "A list of clients on specified node"}).

-rest_api(#{name   => lookup_client,
            method => 'GET',
            path   => "/clients/:bin:clientid",
            func   => lookup,
            descr  => "Lookup a client in the cluster"}).

-rest_api(#{name   => lookup_node_client,
            method => 'GET',
            path   => "nodes/:atom:node/clients/:bin:clientid",
            func   => lookup,
            descr  => "Lookup a client on the node"}).

-rest_api(#{name   => lookup_client_via_username,
            method => 'GET',
            path   => "/clients/username/:bin:username",
            func   => lookup,
            descr  => "Lookup a client via username in the cluster"
           }).

-rest_api(#{name   => lookup_node_client_via_username,
            method => 'GET',
            path   => "/nodes/:atom:node/clients/username/:bin:username",
            func   => lookup,
            descr  => "Lookup a client via username on the node "
           }).

-rest_api(#{name   => kickout_client,
            method => 'DELETE',
            path   => "/clients/:bin:clientid",
            func   => kickout,
            descr  => "Kick out the client in the cluster"}).

-rest_api(#{name   => clean_acl_cache,
            method => 'DELETE',
            path   => "/clients/:bin:clientid/acl_cache",
            func   => clean_acl_cache,
            descr  => "Clear the ACL cache of a specified client in the cluster"}).

-rest_api(#{name   => list_acl_cache,
            method => 'GET',
            path   => "/clients/:bin:clientid/acl_cache",
            func   => list_acl_cache,
            descr  => "List the ACL cache of a specified client in the cluster"}).

-import(emqx_mgmt_util, [ ntoa/1
                        , strftime/1
                        ]).

-export([ list/2
        , lookup/2
        , kickout/2
        , clean_acl_cache/2
        , list_acl_cache/2
        ]).

list(Bindings, Params) when map_size(Bindings) == 0 ->
    return({ok, emqx_mgmt_api:cluster_query(Params, ?CLIENT_QS_SCHEMA, fun query/3)});

list(#{node := Node}, Params) when Node =:= node() ->
    return({ok, emqx_mgmt_api:node_query(Node, Params, ?CLIENT_QS_SCHEMA, fun query/3)});

list(Bindings = #{node := Node}, Params) ->
    case rpc:call(Node, ?MODULE, list, [Bindings, Params]) of
        {badrpc, Reason} -> return({error, ?ERROR1, Reason});
        Res -> Res
    end.

lookup(#{node := Node, clientid := ClientId}, _Params) ->
    return({ok, emqx_mgmt:lookup_client(Node, {clientid, http_uri:decode(ClientId)}, fun format/1)});

lookup(#{clientid := ClientId}, _Params) ->
    return({ok, emqx_mgmt:lookup_client({clientid, http_uri:decode(ClientId)}, fun format/1)});

lookup(#{node := Node, username := Username}, _Params) ->
    return({ok, emqx_mgmt:lookup_client(Node, {username, http_uri:decode(Username)}, fun format/1)});

lookup(#{username := Username}, _Params) ->
    return({ok, emqx_mgmt:lookup_client({username, http_uri:decode(Username)}, fun format/1)}).

kickout(#{clientid := ClientId}, _Params) ->
    case emqx_mgmt:kickout_client(http_uri:decode(ClientId)) of
        ok -> return();
        {error, not_found} -> return({error, ?ERROR12, not_found});
        {error, Reason} -> return({error, ?ERROR1, Reason})
    end.

clean_acl_cache(#{clientid := ClientId}, _Params) ->
    case emqx_mgmt:clean_acl_cache(http_uri:decode(ClientId)) of
        ok -> return();
        {error, not_found} -> return({error, ?ERROR12, not_found});
        {error, Reason} -> return({error, ?ERROR1, Reason})
    end.

list_acl_cache(#{clientid := ClientId}, _Params) ->
    case emqx_mgmt:list_acl_cache(http_uri:decode(ClientId)) of
        {error, not_found} -> return({error, ?ERROR12, not_found});
        {error, Reason} -> return({error, ?ERROR1, Reason});
        Caches -> return({ok, [format_acl_cache(Cache) || Cache <- Caches]})
    end.

%%--------------------------------------------------------------------
%% Format

format(Items) when is_list(Items) ->
    [format(Item) || Item <- Items];
format(Key) when is_tuple(Key) ->
    format(emqx_mgmt:item(client, Key));
format(Data) when is_map(Data)->
    {IpAddr, Port} = maps:get(peername, Data),
    ConnectedAt = maps:get(connected_at, Data),
    CreatedAt = maps:get(created_at, Data),
    Data1 = maps:without([peername], Data),
    maps:merge(Data1#{node         => node(),
                      ip_address   => iolist_to_binary(ntoa(IpAddr)),
                      port         => Port,
                      connected_at => iolist_to_binary(strftime(ConnectedAt div 1000)),
                      created_at   => iolist_to_binary(strftime(CreatedAt div 1000))},
               case maps:get(disconnected_at, Data, undefined) of
                   undefined -> #{};
                   DisconnectedAt -> #{disconnected_at => iolist_to_binary(strftime(DisconnectedAt div 1000))}
               end).

format_acl_cache({{PubSub, Topic}, {AclResult, Timestamp}}) ->
    #{access => PubSub,
      topic => Topic,
      result => AclResult,
      updated_time => Timestamp}.

%%--------------------------------------------------------------------
%% Query Functions
%%--------------------------------------------------------------------

query({Qs, []}, Start, Limit) ->
    Ms = qs2ms_k(Qs),
    emqx_mgmt_api:select_table(emqx_channel_info, Ms, Start, Limit, fun format/1);

query({Qs, Fuzzy}, Start, Limit) ->
    Ms = qs2ms(Qs),
    MatchFun = match_fun(Ms, Fuzzy),
    emqx_mgmt_api:traverse_table(emqx_channel_info, MatchFun, Start, Limit, fun format/1).

%%--------------------------------------------------------------------
%% Match funcs

match_fun(Ms, Fuzzy) ->
    MsC = ets:match_spec_compile(Ms),
    REFuzzy = lists:map(fun({K, like, S}) ->
                  {ok, RE} = re:compile(S),
                  {K, like, RE}
              end, Fuzzy),
    fun(Rows) ->
         case ets:match_spec_run(Rows, MsC) of
             [] -> [];
             Ls ->
                 lists:filtermap(fun(E) ->
                    case run_fuzzy_match(E, REFuzzy) of
                        false -> false;
                        true -> {true, element(1, E)}
                    end end, Ls)
         end
    end.

run_fuzzy_match(_, []) ->
    true;
run_fuzzy_match(E = {_, #{clientinfo := ClientInfo}, _}, [{Key, _, RE}|Fuzzy]) ->
    Val = case maps:get(Key, ClientInfo, "") of
              undefined -> "";
              V -> V
          end,
    re:run(Val, RE, [{capture, none}]) == match andalso run_fuzzy_match(E, Fuzzy).

%%--------------------------------------------------------------------
%% QueryString to Match Spec

-spec qs2ms(list()) -> ets:match_spec().
qs2ms(Qs) ->
    {MtchHead, Conds} = qs2ms(Qs, 2, {#{}, []}),
    [{{'$1', MtchHead, '_'}, Conds, ['$_']}].

qs2ms_k(Qs) ->
    {MtchHead, Conds} = qs2ms(Qs, 2, {#{}, []}),
    [{{'$1', MtchHead, '_'}, Conds, ['$1']}].

qs2ms([], _, {MtchHead, Conds}) ->
    {MtchHead, lists:reverse(Conds)};

qs2ms([{Key, '=:=', Value} | Rest], N, {MtchHead, Conds}) ->
    NMtchHead = emqx_mgmt_util:merge_maps(MtchHead, ms(Key, Value)),
    qs2ms(Rest, N, {NMtchHead, Conds});
qs2ms([Qs | Rest], N, {MtchHead, Conds}) ->
    Holder = binary_to_atom(iolist_to_binary(["$", integer_to_list(N)]), utf8),
    NMtchHead = emqx_mgmt_util:merge_maps(MtchHead, ms(element(1, Qs), Holder)),
    NConds = put_conds(Qs, Holder, Conds),
    qs2ms(Rest, N+1, {NMtchHead, NConds}).

put_conds({_, Op, V}, Holder, Conds) ->
    [{Op, Holder, V} | Conds];
put_conds({_, Op1, V1, Op2, V2}, Holder, Conds) ->
    [{Op2, Holder, V2},
     {Op1, Holder, V1} | Conds].

ms(clientid, X) ->
    #{clientinfo => #{clientid => X}};
ms(username, X) ->
    #{clientinfo => #{username => X}};
ms(zone, X) ->
    #{clientinfo => #{zone => X}};
ms(ip_address, X) ->
    #{clientinfo => #{ip_address => X}};
ms(conn_state, X) ->
    #{conn_state => X};
ms(clean_start, X) ->
    #{conninfo => #{clean_start => X}};
ms(proto_name, X) ->
    #{conninfo => #{proto_name => X}};
ms(proto_ver, X) ->
    #{conninfo => #{proto_ver => X}};
ms(connected_at, X) ->
    #{conninfo => #{connected_at => X}};
ms(created_at, X) ->
    #{session => #{created_at => X}}.

%%--------------------------------------------------------------------
%% EUnits
%%--------------------------------------------------------------------

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

params2qs_test() ->
    QsSchema = element(2, ?CLIENT_QS_SCHEMA),
    Params = [{<<"clientid">>, <<"abc">>},
              {<<"username">>, <<"def">>},
              {<<"zone">>, <<"external">>},
              {<<"ip_address">>, <<"127.0.0.1">>},
              {<<"conn_state">>, <<"connected">>},
              {<<"clean_start">>, true},
              {<<"proto_name">>, <<"MQTT">>},
              {<<"proto_ver">>, 4},
              {<<"_gte_created_at">>, 1},
              {<<"_lte_created_at">>, 5},
              {<<"_gte_connected_at">>, 1},
              {<<"_lte_connected_at">>, 5},
              {<<"_like_clientid">>, <<"a">>},
              {<<"_like_username">>, <<"e">>}
             ],
    ExpectedMtchHead =
        #{clientinfo => #{clientid => <<"abc">>,
                          username => <<"def">>,
                          zone => external,
                          ip_address => {127,0,0,1}
                         },
          conn_state => connected,
          conninfo => #{clean_start => true,
                        proto_name => <<"MQTT">>,
                        proto_ver => 4,
                        connected_at => '$3'},
          session => #{created_at => '$2'}},
    ExpectedCondi = [{'>=','$2', 1},
                     {'=<','$2', 5},
                     {'>=','$3', 1},
                     {'=<','$3', 5}],
    {10, {Qs1, []}} = emqx_mgmt_api:params2qs(Params, QsSchema),
    [{{'$1', MtchHead, _}, Condi, _}] = qs2ms(Qs1),
    ?assertEqual(ExpectedMtchHead, MtchHead),
    ?assertEqual(ExpectedCondi, Condi),

    [{{'$1', #{}, '_'}, [], ['$_']}] = qs2ms([]),
    [{{'$1', #{}, '_'}, [], ['$1']}] = qs2ms_k([]).

-endif.
