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

-module(emqx_dashboard_api).

-include("emqx_dashboard.hrl").

-import(minirest, [return/0, return/1]).

-rest_api(#{name   => auth_user,
            method => 'POST',
            path   => "/auth",
            func   => auth,
            descr  => "Authenticate an user"
           }).

-rest_api(#{name   => create_user,
            method => 'POST',
            path   => "/users/",
            func   => create,
            descr  => "Create an user"
           }).

-rest_api(#{name   => list_users,
            method => 'GET',
            path   => "/users/",
            func   => list,
            descr  => "List users"
           }).

-rest_api(#{name   => update_user,
            method => 'PUT',
            path   => "/users/:bin:name",
            func   => update,
            descr  => "Update an user"
           }).

-rest_api(#{name   => delete_user,
            method => 'DELETE',
            path   => "/users/:bin:name",
            func   => delete,
            descr  => "Delete an user"
           }).

-rest_api(#{name   => change_pwd,
            method => 'PUT',
            path   => "/change_pwd/:bin:username",
            func   => change_pwd,
            descr  => "Change password for an user"
           }).

-export([ list/2
        , create/2
        , update/2
        , delete/2
        , auth/2
        , change_pwd/2
        ]).

-define(EMPTY(V), (V == undefined orelse V == <<>>)).

auth(_Bindings, Params) when is_map(Params) -> auth(_Bindings, maps:to_list(Params));
auth(_Bindings, Params) ->
    Username = maps:get(<<"username">>, Params),
    Password = maps:get(<<"password">>, Params),
    return(emqx_dashboard_admin:check(Username, Password)).

change_pwd(#{username := Username}, Params) ->
    OldPwd = maps:get(<<"old_pwd">>, Params),
    NewPwd = maps:get(<<"new_pwd">>, Params),
    return(emqx_dashboard_admin:change_password(Username, OldPwd, NewPwd)).

create(_Bindings, Params) ->
    Username = maps:get(<<"username">>, Params),
    Password = maps:get(<<"password">>, Params),
    Tags = maps:get(<<"tags">>, Params),
    return(case ?EMPTY(Username) orelse ?EMPTY(Password) of
               true  -> {error, <<"Username or password undefined">>};
               false -> emqx_dashboard_admin:add_user(Username, Password, Tags)
           end).

list(_Bindings, _Params) ->
    return({ok, [row(User) || User <- emqx_dashboard_admin:all_users()]}).

update(#{name := Username}, Params) ->
    Tags = maps:get(<<"tags">>, Params),
    return(emqx_dashboard_admin:update_user(Username, Tags)).

delete(#{name := <<"admin">>}, _Params) ->
    return({error, <<"Cannot delete admin">>});

delete(#{name := Username}, _Params) ->
    return(emqx_dashboard_admin:remove_user(Username)).

row(#mqtt_admin{username = Username, tags = Tags}) ->
    #{username => Username, tags => Tags}.

