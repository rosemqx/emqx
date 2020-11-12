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

-module(emqx_mgmt_http).

-export([ handle_request/2]).

-export([init/2, http_handlers/0, is_authorized/1]).

-include("emqx.hrl").

-ifdef(TEST).
-define(EXCEPT, []).
-else.
-define(EXCEPT, [add_app, del_app, list_apps, lookup_app, update_app]).
-endif.

http_handlers() ->
    Plugins = lists:map(fun(Plugin) -> Plugin#plugin.name end, emqx_plugins:list()),
    Cfg = #{apps => Plugins, modules=> [], except => ?EXCEPT, filter => fun filter/1},
    Hnd = minirest_handler:init(Cfg),
    [{"/api/v4", Hnd, [{authorization, fun is_authorized/1}]}].

%%--------------------------------------------------------------------
%% Handle 'status' request
%%--------------------------------------------------------------------
init(Req, Opts) ->
    Origin = case cowboy_req:header(<<"origin">>, Req, <<"*">>) of {O,_}->O;X -> X end,
    Req1 = cowboy_req:set_resp_header(<<"access-control-allow-origin">>,Origin,Req),
    Req2 = handle_request(cowboy_req:path(Req1), Req1),
    {ok, Req2, Opts}.

handle_request(Path, Req) ->
    handle_request(cowboy_req:method(Req), Path, Req).

handle_request(<<"GET">>, <<"/status">>, Req) ->
    {InternalStatus, _ProvidedStatus} = init:get_status(),
    AppStatus = case lists:keysearch(emqx, 1, application:which_applications()) of
        false         -> not_running;
        {value, _Val} -> running
    end,
    Status = io_lib:format("Node ~s is ~s~nemqx is ~s",
                            [node(), InternalStatus, AppStatus]),
    cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain">>}, Status, Req);

handle_request(_Method, _Path, Req) ->
    cowboy_req:reply(400, #{<<"content-type">> => <<"text/plain">>}, <<"Not found.">>, Req).

is_authorized(Req) -> is_authorized(cowboy_req:path(Req), Req).
is_authorized(<<"/api/v4/auth">>, _Req) -> true;
is_authorized(_Path, Req) -> basic_auth(Req).

basic_auth(Req) ->
    case cowboy_req:parse_header(<<"authorization">>, Req) of
        {basic, AppId = Username, AppSecret = Password} ->
            case emqx_admin:check(iolist_to_binary(Username), iolist_to_binary(Password)) of
                ok -> true;
                {error, Reason} ->
                    io:format("Authorization Failure: username=~s, reason=~p~n", [Username, Reason]),
                    emqx_mgmt_auth:is_authorized(AppId, AppSecret)
            end;
        _  -> false
    end.

filter(#{app := App}) ->
    case emqx_plugins:find_plugin(App) of
        false -> false;
        Plugin -> Plugin#plugin.active
    end.
