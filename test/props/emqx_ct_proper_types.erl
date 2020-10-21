%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% The proper types extension for EMQ X

-module(emqx_ct_proper_types).

-include_lib("proper/include/proper.hrl").

%% High level Types
-export([ conninfo/0
        , clientinfo/0
        , sessioninfo/0
        , connack_return_code/0
        , message/0
        , topictab/0
        , topic/0
        , systopic/0
        , subopts/0
        , nodename/0
        , normal_topic/0
        , normal_topic_filter/0
        ]).

%% Basic Types
-export([ url/0
        , ip/0
        , port/0
        ]).

%% Iterators
-export([ nof/1
        ]).

%%--------------------------------------------------------------------
%% Types High level
%%--------------------------------------------------------------------

%% Type defined emqx_types.erl - conninfo()
conninfo() ->
    Keys = [{socktype,  socktype()},
            {sockname, peername()},
            {peername, peername()},
            {peercert, peercert()},
            {conn_mod, conn_mod()},
            {proto_name, proto_name()},
            {proto_ver, non_neg_integer()},
            {clean_start, boolean()},
            {clientid, clientid()},
            {username, username()},
            {conn_props, properties()},
            {connected, boolean()},
            {connected_at, timestamp()},
            {keepalive, range(0, 16#ffff)},
            {receive_maximum, non_neg_integer()},
            {expiry_interval, non_neg_integer()}],
    ?LET({Ks, M}, {Keys, map(atom(), term())},
         begin
             maps:merge(maps:from_list(Ks), M)
         end).

clientinfo() ->
    Keys = [{zone, zone()},
            {protocol, protocol()},
            {peerhost, ip()},
            {sockport, port()},
            {clientid, clientid()},
            {username, username()},
            {is_bridge, boolean()},
            {is_supuser, boolean()},
            {mountpoint, maybe(utf8())},
            {ws_cookie, maybe(list())}
            % password,
            % auth_result,
            % anonymous,
            % cn,
            % dn,
            ],
    ?LET({Ks, M}, {Keys, map(atom(), term())},
         begin
             maps:merge(maps:from_list(Ks), M)
         end).

%% See emqx_session:session() type define
sessioninfo() ->
    ?LET(Session, {session,
                    subscriptions(),    % subscriptions
                    non_neg_integer(),  % max_subscriptions
                    boolean(),          % upgrade_qos
                    inflight(),         % emqx_inflight:inflight()
                    mqueue(),           % emqx_mqueue:mqueue()
                    packet_id(),        % next_pkt_id
                    safty_timeout(),    % retry_interval
                    awaiting_rel(),     % awaiting_rel
                    non_neg_integer(),  % max_awaiting_rel
                    safty_timeout(),    % await_rel_timeout
                    timestamp()         % created_at
                  },
         emqx_session:info(Session)).

subscriptions() ->
    ?LET(L, list({topic(), subopts()}), maps:from_list(L)).

inflight() ->
    ?LET(MaxLen, non_neg_integer(),
        begin
            ?LET(Msgs, limited_list(MaxLen, {packet_id(), message(), timestamp()}),
                begin
                    lists:foldl(fun({PktId, Msg, Ts}, Ift) ->
                        try
                            emqx_inflight:insert(PktId, {Msg, Ts}, Ift)
                        catch _:_ ->
                            Ift
                        end
                    end, emqx_inflight:new(MaxLen), Msgs)
                end)
        end).

mqueue() ->
    ?LET({MaxLen, IsStoreQos0}, {non_neg_integer(), boolean()},
         begin
            ?LET(Msgs, limited_list(MaxLen, message()),
                 begin
                    Q = emqx_mqueue:init(#{max_len => MaxLen, store_qos0 => IsStoreQos0}),
                    lists:foldl(fun(Msg, Acc) ->
                        {_Dropped, NQ} = emqx_mqueue:in(Msg, Acc),
                        NQ
                    end, Q, Msgs)
                 end)
         end).

message() ->
    {message, emqx_guid:gen(), qos(), from(), flags(), map(atom(), any()), topic(), payload(), timestamp()}.

%% @private
flags() ->
    ?LET({Dup, Retain}, {boolean(), boolean()}, #{dup => Dup, retain => Retain}).

packet_id() ->
    range(1, 16#ffff).

awaiting_rel() ->
    ?LET(L, list({packet_id(), timestamp()}), maps:from_list(L)).

connack_return_code() ->
    oneof([ success
          , protocol_error
          , client_identifier_not_valid
          , bad_username_or_password
          , bad_clientid_or_password
          , username_or_password_undefined
          , password_error
          , not_authorized
          , server_unavailable
          , server_busy
          , banned
          , bad_authentication_method
          ]).

topictab() ->
    non_empty(list({topic(), subopts()})).

topic() ->
    oneof([normal_topic(),
           normal_topic_filter(),
           systopic_broker(), systopic_present(), systopic_stats(),
           systopic_metrics(), systopic_alarms(), systopic_mon(),
           sharetopic()]).

subopts() ->
    ?LET({Nl, Qos, Rap, Rh},
         {range(0, 1), qos(),
          range(0, 1), range(0, 1)},
         #{nl => Nl, qos => Qos, rap => Rap, rh => Rh}).

qos() ->
    range(0, 2).

from() ->
    oneof([atom(), binary()]).

payload() ->
    binary().

safty_timeout() ->
    non_neg_integer().

nodename() ->
    ?LET({Name, Ip}, {non_empty(list(latin_char())), ip()},
        begin
            binary_to_atom(iolist_to_binary([Name, "@", inet:ntoa(Ip)]), utf8)
        end).

systopic() ->
    oneof(
      [systopic_broker(), systopic_present(), systopic_stats(),
       systopic_metrics(), systopic_alarms(), systopic_mon()]).

systopic_broker() ->
    Topics = [<<"">>, <<"version">>, <<"uptime">>, <<"datetime">>, <<"sysdescr">>],
    ?LET({Nodename, T},
         {nodename(), oneof(Topics)},
        begin
            case byte_size(T) of
                0 -> <<"$SYS/brokers">>;
                _ ->
                    <<"$SYS/brokers/", (ensure_bin(Nodename))/binary, "/", T/binary>>
            end
        end).

systopic_present() ->
    ?LET({Nodename, ClientId, T},
         {nodename(), clientid(), oneof([<<"connected">>, <<"disconnected">>])},
        begin
            <<"$SYS/brokers/", (ensure_bin(Nodename))/binary, "/clients/", (ensure_bin(ClientId))/binary, "/", T/binary>>
        end).

systopic_stats() ->
    Topics = [<<"connections/max">>, <<"connections/count">>,
              <<"suboptions/max">>, <<"suboptions/count">>,
              <<"subscribers/max">>, <<"subscribers/count">>,
              <<"subscriptions/max">>, <<"subscriptions/count">>,
              <<"subscriptions/shared/max">>, <<"subscriptions/shared/count">>,
              <<"topics/max">>, <<"topics/count">>,
              <<"routes/max">>, <<"routes/count">>
              ],
    ?LET({Nodename, T},
         {nodename(), oneof(Topics)},
         <<"$SYS/brokers/", (ensure_bin(Nodename))/binary, "/stats/", T/binary>>).

systopic_metrics() ->
    Topics = [<<"bytes/received">>, <<"bytes/sent">>,
              <<"packets/received">>, <<"packets/sent">>,
              <<"packets/connect/received">>, <<"packets/connack/sent">>,
              <<"packets/publish/received">>, <<"packets/publish/sent">>,
              <<"packets/publish/error">>, <<"packets/publish/auth_error">>,
              <<"packets/publish/dropped">>,
              <<"packets/puback/received">>, <<"packets/puback/sent">>,
              <<"packets/puback/inuse">>, <<"packets/puback/missed">>,
              <<"packets/pubrec/received">>, <<"packets/pubrec/sent">>,
              <<"packets/pubrec/inuse">>, <<"packets/pubrec/missed">>,
              <<"packets/pubrel/received">>, <<"packets/pubrel/sent">>,
              <<"packets/pubrel/missed">>,
              <<"packets/pubcomp/received">>, <<"packets/pubcomp/sent">>,
              <<"packets/pubcomp/inuse">>, <<"packets/pubcomp/missed">>,
              <<"packets/subscribe/received">>, <<"packets/subscribe/error">>,
              <<"packets/subscribe/auth_error">>, <<"packets/suback/sent">>,
              <<"packets/unsubscribe/received">>, <<"packets/unsuback/sent">>,
              <<"packets/pingreq/received">>, <<"packets/pingresp/sent">>,
              <<"packets/disconnect/received">>, <<"packets/disconnect/sent">>,
              <<"packets/auth/received">>, <<"packets/auth/sent">>,
              <<"messages/received">>, <<"messages/sent">>,
              <<"messages/qos0/received">>, <<"messages/qos0/sent">>,
              <<"messages/qos1/received">>, <<"messages/qos1/sent">>,
              <<"messages/qos2/received">>, <<"messages/qos2/sent">>,
              <<"messages/publish">>, <<"messages/dropped">>,
              <<"messages/dropped/expired">>, <<"messages/dropped/no_subscribers">>,
              <<"messages/forward">>, <<"messages/retained">>,
              <<"messages/delayed">>, <<"messages/delivered">>,
              <<"messages/acked">>],
    ?LET({Nodename, T},
         {nodename(), oneof(Topics)},
         <<"$SYS/brokers/", (ensure_bin(Nodename))/binary, "/metrics/", T/binary>>).

systopic_alarms() ->
    ?LET({Nodename, T},
         {nodename(), oneof([<<"alert">>, <<"clear">>])},
         <<"$SYS/brokers/", (ensure_bin(Nodename))/binary, "/alarms/", T/binary>>).

systopic_mon() ->
    Topics = [<<"long_gc">>, <<"long_schedule">>,
              <<"large_heap">>, <<"busy_port">>, <<"busy_dist_port">>],
    ?LET({Nodename, T},
         {nodename(), oneof(Topics)},
         <<"$SYS/brokers/", (ensure_bin(Nodename))/binary, "/sysmon/", T/binary>>).

sharetopic() ->
    ?LET({Type, Grp, T},
         {oneof([<<"$queue">>, <<"$share">>]), list(latin_char()), normal_topic()},
         <<Type/binary, "/", (iolist_to_binary(Grp))/binary, "/", T/binary>>).

normal_topic() ->
    ?LET(L, list(frequency([{3, latin_char()}, {1, $/}])),
         list_to_binary(L)).

normal_topic_filter() ->
    ?LET({L, Wild}, {list(list(latin_char())), oneof(['#', '+'])},
         begin
             case Wild of
                 '#' ->
                     case L of
                         [] -> <<"#">>;
                         _ ->  iolist_to_binary([lists:join("/", L), "/#"])
                     end;
                 '+' ->
                    case L of
                        [] -> <<"+">>;
                        _ ->
                            L1 = [case rand:uniform(3) == 1 of
                                      true -> "+";
                                      _ -> E
                                  end || E <- L],
                            iolist_to_binary(lists:join("/", L1))
                    end
             end
         end).

%%--------------------------------------------------------------------
%% Basic Types
%%--------------------------------------------------------------------

maybe(T) ->
    oneof([undefined, T]).

socktype() ->
    oneof([tcp, udp, ssl,  proxy, atom()]).

peername() ->
    {ip(), port()}.

peercert() ->
    %% TODO: cert?
    oneof([nossl, undefined]).

conn_mod() ->
    oneof([emqx_connection, emqx_ws_connection, emqx_coap_mqtt_adapter,
           emqx_sn_gateway, emqx_lwm2m_protocol, atom()]).

proto_name() ->
    oneof([<<"MQTT">>, <<"MQTT-SN">>, <<"CoAP">>, <<"LwM2M">>, utf8()]).

clientid() ->
    utf8().

username() ->
    maybe(utf8()).

properties() ->
    map(latin_atom(), binary()).

%% millisecond
timestamp() ->
    %% 12h <- Now -> 12h
    ?LET(Offset, range(-43200, 43200), erlang:system_time(millisecond) + Offset).

zone() ->
    oneof([external, internal, atom()]).

protocol() ->
    oneof([mqtt, 'mqtt-sn', coap, lwm2m, atom()]).

url() ->
    ?LET({Schema, IP, Port, Path}, {oneof(["http://", "https://"]), ip(), port(), http_path()},
        begin
            lists:concat([Schema, inet:ntoa(IP), ":", integer_to_list(Port), "/", Path])
        end).

ip() ->
    oneof([ipv4(), ipv6(), ipv6_from_v4()]).

ipv4() ->
    ?LET(IP, {range(1, 16#ff), range(0, 16#ff),
              range(0, 16#ff), range(0, 16#ff)}, IP).

ipv6() ->
    ?LET(IP, {range(0, 16#ff), range(0, 16#ff),
              range(0, 16#ff), range(0, 16#ff),
              range(0, 16#ff), range(0, 16#ff),
              range(0, 16#ff), range(0, 16#ff)}, IP).

ipv6_from_v4() ->
    ?LET(IP, {range(1, 16#ff), range(0, 16#ff),
              range(0, 16#ff), range(0, 16#ff)},
         inet:ipv4_mapped_ipv6_address(IP)).

port() ->
    ?LET(Port, range(1, 16#ffff), Port).

http_path() ->
    list(frequency([{3, latin_char()},
                    {1, $/}])).

latin_char() ->
    oneof([integer($0, $9), integer($A, $Z), integer($a, $z)]).

latin_atom() ->
    ?LET(Cs, list(latin_char()), list_to_atom(Cs)).

%%--------------------------------------------------------------------
%% Iterators
%%--------------------------------------------------------------------

nof(Ls) when is_list(Ls) ->
    Len = length(Ls),
    ?LET(N, range(0, Len),
         begin
            Ns = rand_nl(N, Len, []),
            [lists:nth(I, Ls) || I <- Ns]
         end).

limited_list(0, T) ->
    list(T);

limited_list(N, T) ->
    ?LET(N2, range(0, N),
         begin
             [T || _ <- lists:seq(1, N2)]
         end).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

-compile({inline, rand_nl/3}).

rand_nl(0, _, Acc) ->
    Acc;
rand_nl(N, L, Acc) ->
    R = rand:uniform(L),
    case lists:member(R, Acc) of
        true -> rand_nl(N, L, Acc);
        _ -> rand_nl(N-1, L, [R|Acc])
    end.

ensure_bin(A) when is_atom(A) ->
    atom_to_binary(A, utf8);
ensure_bin(B) when is_binary(B) ->
    B.

