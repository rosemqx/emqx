%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc
%% A stateful interaction between a Client and a Server. Some Sessions
%% last only as long as the Network Connection, others can span multiple
%% consecutive Network Connections between a Client and a Server.
%%
%% The Session State in the Server consists of:
%%
%% The existence of a Session, even if the rest of the Session State is empty.
%%
%% The Clients subscriptions, including any Subscription Identifiers.
%%
%% QoS 1 and QoS 2 messages which have been sent to the Client, but have not
%% been completely acknowledged.
%%
%% QoS 1 and QoS 2 messages pending transmission to the Client and OPTIONALLY
%% QoS 0 messages pending transmission to the Client.
%%
%% QoS 2 messages which have been received from the Client, but have not been
%% completely acknowledged.The Will Message and the Will Delay Interval
%%
%% If the Session is currently not connected, the time at which the Session
%% will end and Session State will be discarded.
%% @end

-module(emqx_session).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("logger.hrl").

-export([init/1]).
-export([info/1, attrs/1]).
-export([stats/1]).
-export([resume/2, discard/2]).
-export([update_expiry_interval/2]).
-export([subscribe/2, subscribe/3]).
-export([publish/3]).
-export([puback/2, puback/3]).
-export([pubrec/3]).
-export([pubrel/3, pubcomp/3]).
-export([unsubscribe/2, unsubscribe/4]).
-export([close/1]).

-import(emqx_zone, [get_env/2, get_env/3]).

-record(session, {
          %% Clean Start Flag
          clean_start = false :: boolean(),

          binding = local,

          conn_pid :: pid(),

          old_conn_pid :: pid(),

          %% ClientId: Identifier of Session
          client_id :: binary(),

          %% Username
          username :: binary() | undefined,

          %% Max subscriptions
          max_subscriptions :: non_neg_integer(),

          %% Client’s Subscriptions.
          subscriptions :: map(),

          %% Upgrade QoS?
          upgrade_qos = false :: boolean(),

          %% Client <- Broker: Inflight QoS1, QoS2 messages sent to the client but unacked.
          inflight :: emqx_inflight:inflight(),

          %% Max Inflight Size. DEPRECATED: Get from inflight
          %% max_inflight = 32 :: non_neg_integer(),

          %% Retry interval for redelivering QoS1/2 messages
          retry_interval = 20000 :: timeout(),

          %% Retry Timer
          retry_timer :: reference() | undefined,

          %% All QoS1, QoS2 messages published to when client is disconnected.
          %% QoS 1 and QoS 2 messages pending transmission to the Client.
          %%
          %% Optionally, QoS 0 messages pending transmission to the Client.
          mqueue :: emqx_mqueue:mqueue(),

          %% Client -> Broker: Inflight QoS2 messages received from client and waiting for pubrel.
          awaiting_rel :: map(),

          %% Max Packets Awaiting PUBREL
          max_awaiting_rel = 100 :: non_neg_integer(),

          %% Awaiting PUBREL Timeout
          await_rel_timeout = 20000 :: timeout(),

          %% Awaiting PUBREL Timer
          await_rel_timer :: reference() | undefined,

          %% Session Expiry Interval
          expiry_interval = 7200 :: timeout(),

          %% Expired Timer
          expiry_timer :: reference() | undefined,

          %% Created at
          created_at :: erlang:timestamp(),

          will_msg :: emqx:message(),

          will_delay_timer :: reference() | undefined,

          next_pkt_id :: pos_integer()
         }).

-type(session() :: #session{}).
-type(attr() :: {atom(), term()}).

-export_type([session/0, attr/0]).

%% @doc Get session info
-spec(info(session()) -> list({atom(), term()})).
info(Session = #session{max_subscriptions = MaxSubscriptions,
                        subscriptions = Subscriptions,
                        upgrade_qos = UpgradeQoS,
                        inflight = Inflight,
                        retry_interval = RetryInterval,
                        mqueue = MQueue,
                        awaiting_rel = AwaitingRel,
                        max_awaiting_rel = MaxAwaitingRel,
                        await_rel_timeout = AwaitRelTimeout
                       }) ->
    attrs(Session) ++ [{max_subscriptions, MaxSubscriptions},
                       {subscriptions, Subscriptions},
                       {upgrade_qos, UpgradeQoS},
                       {inflight, Inflight},
                       {retry_interval, RetryInterval},
                       {mqueue_len, MQueue},
                       {awaiting_rel, AwaitingRel},
                       {max_awaiting_rel, MaxAwaitingRel},
                       {await_rel_timeout, AwaitRelTimeout}].

%% @doc Get session attrs
-spec(attrs(session()) -> list({atom(), term()})).
attrs(#session{clean_start = CleanStart,
               client_id = ClientId,
               username = Username,
               expiry_interval = ExpiryInterval,
               created_at = CreatedAt}) ->
    [{clean_start, CleanStart},
     {client_id, ClientId},
     {username, Username},
     {expiry_interval, ExpiryInterval div 1000},
     {created_at, CreatedAt}].

%% @doc Get session stats
-spec(stats(session()) -> list({atom(), non_neg_integer()})).
stats(#session{max_subscriptions = MaxSubscriptions,
               subscriptions = Subscriptions,
               inflight = Inflight,
               mqueue = MQueue,
               max_awaiting_rel = MaxAwaitingRel,
               awaiting_rel = AwaitingRel}) ->
    [{max_subscriptions, MaxSubscriptions},
     {subscriptions_count, maps:size(Subscriptions)},
     {max_inflight, emqx_inflight:max_size(Inflight)},
     {inflight_len, emqx_inflight:size(Inflight)},
     {max_mqueue, emqx_mqueue:max_len(MQueue)},
     {mqueue_len, emqx_mqueue:len(MQueue)},
     {mqueue_dropped, emqx_mqueue:dropped(MQueue)},
     {max_awaiting_rel, MaxAwaitingRel},
     {awaiting_rel_len, maps:size(AwaitingRel)},
     {deliver_msg, emqx_pd:get_counter(deliver_stats)},
     {enqueue_msg, emqx_pd:get_counter(enqueue_stats)}].

%%------------------------------------------------------------------------------
%% PubSub API
%%------------------------------------------------------------------------------

%% SUBSCRIBE:
-spec(subscribe(session(), list({emqx_topic:topic(), emqx_types:subopts()}))
      -> {ok, list(emqx_mqtt_types:reason_code()), session()}).
subscribe(Session, RawTopicFilters) when is_list(RawTopicFilters) ->
    TopicFilters = [emqx_topic:parse(RawTopic, maps:merge(?DEFAULT_SUBOPTS, SubOpts))
                    || {RawTopic, SubOpts} <- RawTopicFilters],
    subscribe(Session, #{}, TopicFilters).

-spec(subscribe(session(), emqx_mqtt_types:properties(), emqx_mqtt_types:topic_filters())
      -> {ok, list(emqx_mqtt_types:reason_code()), session()}).
subscribe(Session = #session{client_id = ClientId, subscriptions = Subscriptions},
          Properties, TopicFilters) ->
    {ReasonCodes, Subscriptions1} =
        lists:foldr(fun({Topic, SubOpts = #{qos := QoS}}, {RcAcc, SubMap}) ->
                            {[QoS|RcAcc], case maps:find(Topic, SubMap) of
                                              {ok, SubOpts} ->
                                                  emqx_hooks:run('session.subscribed', [#{client_id => ClientId}, Topic, SubOpts#{first => false}]),
                                                  SubMap;
                                              {ok, _SubOpts} ->
                                                  emqx_broker:set_subopts(Topic, SubOpts),
                                                  %% Why???
                                                  emqx_hooks:run('session.subscribed', [#{client_id => ClientId}, Topic, SubOpts#{first => false}]),
                                                  maps:put(Topic, SubOpts, SubMap);
                                              error ->
                                                  emqx_broker:subscribe(Topic, ClientId, SubOpts),
                                                  emqx_hooks:run('session.subscribed', [#{client_id => ClientId}, Topic, SubOpts#{first => true}]),
                                                  maps:put(Topic, SubOpts, SubMap)
                                          end}
                    end, {[], Subscriptions}, TopicFilters),
        {ok, ReasonCodes, Session#session{subscriptions = Subscriptions1}}.

%% @doc Called by connection processes when publishing messages
-spec(publish(session(), emqx_mqtt_types:packet_id(), emqx_types:message())
      -> emqx_types:deliver_results() | {error, term()}).
publish(_Session, _PacketId, Msg = #message{qos = ?QOS_0}) ->
    %% Publish QoS0 message directly
    emqx_broker:publish(Msg);

publish(_Session, _PacketId, Msg = #message{qos = ?QOS_1}) ->
    %% Publish QoS1 message directly
    emqx_broker:publish(Msg);

%% PUBLISH: This is only to register packetId to session state.
%% The actual message dispatching should be done by the caller (e.g. connection) process.
%% Register QoS2 message packet ID (and timestamp) to session, then publish
publish(Session = #session{awaiting_rel = AwaitingRel},
        PacketId, Msg = #message{qos = ?QOS_2, timestamp = Ts}) ->
    case is_awaiting_full(Session) of
          false ->
              case maps:is_key(PacketId, AwaitingRel) of
                  true ->
                      {error, ?RC_PACKET_IDENTIFIER_IN_USE};
                  false ->
                      emqx_broker:publish(Msg),
                      Session1 = Session#session{awaiting_rel = maps:put(PacketId, Ts, AwaitingRel)},
                      {ok, ensure_await_rel_timer(Session1)}
              end;
          true ->
              ?WARN("Dropped qos2 packet ~w for too many awaiting_rel", [PacketId]),
              emqx_metrics:trans(inc, 'messages/qos2/dropped'),
              {error, ?RC_RECEIVE_MAXIMUM_EXCEEDED}
      end.

%% PUBACK:
-spec(puback(session(), emqx_mqtt_types:packet_id()) -> session()).
puback(Session, PacketId) ->
    puback(Session, PacketId, ?RC_SUCCESS).

-spec(puback(session(), emqx_mqtt_types:packet_id(), emqx_mqtt_types:reason_code()) -> session()).
puback(Session = #session{inflight = Inflight}, PacketId, ReasonCode) ->
    case emqx_inflight:contain(PacketId, Inflight) of
        true ->
            {ok, dequeue(acked(puback, PacketId, Session))};
        false ->
            ?WARN("The PUBACK PacketId ~w is not found", [PacketId]),
            emqx_metrics:trans(inc, 'packets/puback/missed'),
            {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}
    end.

%% PUBREC:
-spec(pubrec(session(), emqx_mqtt_types:packet_id(), emqx_mqtt_types:reason_code())
      -> {ok, session()} | {error, emqx_mqtt_types:reason_code()}).
pubrec(Session = #session{inflight = Inflight}, PacketId, ReasonCode) ->
    case emqx_inflight:contain(PacketId, Inflight) of
        true ->
            {ok, acked(pubrec, PacketId, Session)};
        false ->
            ?WARN("The PUBREC PacketId ~w is not found.", [PacketId]),
            emqx_metrics:trans(inc, 'packets/pubrec/missed'),
            {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}
    end.

%% PUBREL:
-spec(pubrel(session(), emqx_mqtt_types:packet_id(), emqx_mqtt_types:reason_code())
      -> {ok, session()} | {error, emqx_mqtt_types:reason_code()}).
pubrel(Session = #session{awaiting_rel = AwaitingRel}, PacketId, ReasonCode) ->
      case maps:take(PacketId, AwaitingRel) of
          {_Ts, AwaitingRel1} ->
              {ok, Session#session{awaiting_rel = AwaitingRel1}};
          error ->
              ?WARN("The PUBREL PacketId ~w is not found", [PacketId]),
              emqx_metrics:trans(inc, 'packets/pubrel/missed'),
              {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}
      end.

%% PUBCOMP:
-spec(pubcomp(session(), emqx_mqtt_types:packet_id(), emqx_mqtt_types:reason_code())
      -> {ok, session()}).
pubcomp(PacketId, ReasonCode, Session = #session{inflight = Inflight}) ->
    case emqx_inflight:contain(PacketId, Inflight) of
        true ->
            {ok, dequeue(acked(pubcomp, PacketId, Session))};
        false ->
            ?WARN("The PUBCOMP PacketId ~w is not found", [PacketId]),
            emqx_metrics:trans(inc, 'packets/pubcomp/missed'),
            {ok, Session}
    end.

-spec(unsubscribe(session(), emqx_types:topic_table())
      -> {ok, [emqx_mqtt_types:reason_code()], session()}).
unsubscribe(Session, RawTopicFilters) when is_list(RawTopicFilters) ->
   TopicFilters = lists:map(fun({RawTopic, Opts}) ->
                                    emqx_topic:parse(RawTopic, Opts);
                               (RawTopic) when is_binary(RawTopic) ->
                                    emqx_topic:parse(RawTopic)
                            end, RawTopicFilters),
    unsubscribe(undefined, #{}, TopicFilters, Session).

%% UNSUBSCRIBE:
-spec(unsubscribe(session(), emqx_mqtt_types:packet_id(),
                  emqx_mqtt_types:properties(), emqx_mqtt_types:topic_filters())
      -> {ok, [emqx_mqtt_types:reason_code()], session()}).
unsubscribe(Session = #session{client_id = ClientId, subscriptions = Subscriptions},
            PacketId, Properties, TopicFilters) ->
    {ReasonCodes, Subscriptions1} =
        lists:foldr(fun({Topic, _SubOpts}, {Acc, SubMap}) ->
                            case maps:find(Topic, SubMap) of
                                {ok, SubOpts} ->
                                    ok = emqx_broker:unsubscribe(Topic),
                                    emqx_hooks:run('session.unsubscribed', [#{client_id => ClientId}, Topic, SubOpts]),
                                    {[?RC_SUCCESS|Acc], maps:remove(Topic, SubMap)};
                                error ->
                                    {[?RC_NO_SUBSCRIPTION_EXISTED|Acc], SubMap}
                            end
                    end, {[], Subscriptions}, TopicFilters),
    {ok, ReasonCodes, Session#session{subscriptions = Subscriptions1}}.

%% RESUME:
-spec(resume(session(), map()) -> ok).
resume(Session = #session{client_id        = ClientId,
                           conn_pid         = OldConnPid,
                           clean_start      = CleanStart,
                           retry_timer      = RetryTimer,
                           await_rel_timer  = AwaitTimer,
                           expiry_timer     = ExpireTimer,
                           will_delay_timer = WillDelayTimer},
       #{conn_pid        := ConnPid,
         will_msg        := WillMsg,
         expiry_interval := ExpiryInterval,
         max_inflight    := MaxInflight}) ->

    ?INFO("Resumed by connection ~p ", [ConnPid]),

    %% Cancel Timers
    lists:foreach(fun emqx_misc:cancel_timer/1,
                  [RetryTimer, AwaitTimer, ExpireTimer, WillDelayTimer]),

    case kick(ClientId, OldConnPid, ConnPid) of
        ok -> ?WARN("Connection ~p kickout ~p", [ConnPid, OldConnPid]);
        ignore -> ok
    end,

    %%true = link(ConnPid),

    Session1 = Session#session{conn_pid         = ConnPid,
                               %%old_conn_pid     = OldConnPid,
                               clean_start      = false,
                               retry_timer      = undefined,
                               awaiting_rel     = #{},
                               await_rel_timer  = undefined,
                               expiry_timer     = undefined,
                               expiry_interval  = ExpiryInterval,
                               inflight         = emqx_inflight:update_size(MaxInflight, Session#session.inflight),
                               will_delay_timer = undefined,
                               will_msg         = WillMsg},

    %% Clean Session: true -> false???
    CleanStart andalso emqx_sm:set_session_attrs(ClientId, attrs(Session1)),

    emqx_hooks:run('session.resumed', [#{client_id => ClientId}, attrs(Session1)]),

    %% Replay delivery and Dequeue pending messages
    dequeue(retry_delivery(true, Session1)).

%%FIXME later
%% @doc Discard the session
-spec(discard(session(), pid()) -> ok).
discard(#session{conn_pid = undefined}, CPid) ->
    ?WARN("Discarded by ~p", [CPid]),
    ok;

discard(#session{client_id = ClientId, conn_pid = ChanPid}, ByPid) ->
    ?WARN("Channel ~p is discarded by ~p", [ChanPid, ByPid]),
    ChanPid ! {shutdown, discard, {ClientId, ByPid}},
    ok.

-spec(update_expiry_interval(session(), timeout()) -> session()).
update_expiry_interval(Session, Interval) ->
    Session#session{expiry_interval = Interval}.

-spec(close(session()) -> ok).
close(_Session) -> ok.

init(#{zone            := Zone,
       conn_pid        := ConnPid,
       client_id       := ClientId,
       username        := Username,
       clean_start     := CleanStart,
       expiry_interval := ExpiryInterval,
       max_inflight    := MaxInflight,
       will_msg        := WillMsg}) ->
    Session = #session{clean_start       = CleanStart,
                       client_id         = ClientId,
                       username          = Username,
                       conn_pid          = ConnPid,
                       subscriptions     = #{},
                       max_subscriptions = get_env(Zone, max_subscriptions, 0),
                       upgrade_qos       = get_env(Zone, upgrade_qos, false),
                       inflight          = emqx_inflight:new(MaxInflight),
                       mqueue            = init_mqueue(Zone),
                       retry_interval    = get_env(Zone, retry_interval, 0),
                       awaiting_rel      = #{},
                       await_rel_timeout = get_env(Zone, await_rel_timeout),
                       max_awaiting_rel  = get_env(Zone, max_awaiting_rel),
                       expiry_interval   = ExpiryInterval,
                       created_at        = os:timestamp(),
                       will_msg          = WillMsg},
    emqx_logger:set_metadata_client_id(ClientId),
    ok = emqx_sm:register_session(ClientId, self()),
    true = emqx_sm:set_session_attrs(ClientId, attrs(Session)),
    true = emqx_sm:set_session_stats(ClientId, stats(Session)),
    emqx_hooks:run('session.created', [#{client_id => ClientId}, info(Session)]),
    Session.

init_mqueue(Zone) ->
    emqx_mqueue:init(#{max_len => get_env(Zone, max_mqueue_len, 1000),
                       store_qos0 => get_env(Zone, mqueue_store_qos0, true),
                       priorities => get_env(Zone, mqueue_priorities),
                       default_priority => get_env(Zone, mqueue_default_priority)
                      }).


%% Batch dispatch
handle_info({dispatch, Topic, Msgs}, State) when is_list(Msgs) ->
    lists:foldl(
              fun(Msg, St) ->
                  element(2, handle_info({dispatch, Topic, Msg}, St))
              end, State, Msgs);

%% Dispatch message
handle_info({dispatch, Topic, Msg = #message{}}, State) ->
    case emqx_shared_sub:is_ack_required(Msg) andalso not has_connection(State) of
        true ->
            %% Require ack, but we do not have connection
            %% negative ack the message so it can try the next subscriber in the group
            ok = emqx_shared_sub:nack_no_connection(Msg),
            {noreply, State};
        false ->
            handle_dispatch(Topic, Msg, State)
    end;

%% Do nothing if the client has been disconnected.
handle_info({timeout, Timer, retry_delivery}, State = #session{conn_pid = undefined, retry_timer = Timer}) ->
    State#session{retry_timer = undefined};

handle_info({timeout, Timer, retry_delivery}, State = #session{retry_timer = Timer}) ->
    retry_delivery(false, State#session{retry_timer = undefined});

handle_info({timeout, Timer, check_awaiting_rel}, State = #session{await_rel_timer = Timer}) ->
    expire_awaiting_rel(State#session{await_rel_timer = undefined});

handle_info({timeout, Timer, emit_stats}, State = #session{client_id = ClientId}) ->
    emqx_metrics:commit(),
    emqx_sm:set_session_stats(ClientId, stats(State));

handle_info({timeout, Timer, expired}, State = #session{expiry_timer = Timer}) ->
    ?LOG(info, "expired, shutdown now.", []),
    {stop, {shutdown, expired}, State};

handle_info({timeout, Timer, will_delay}, State = #session{will_msg = WillMsg, will_delay_timer = Timer}) ->
    send_willmsg(WillMsg),
    {noreply, State#session{will_msg = undefined}};

%% ConnPid is shutting down by the supervisor.
handle_info({'EXIT', ConnPid, Reason}, State = #session{will_msg = WillMsg, expiry_interval = 0, conn_pid = ConnPid}) ->
    send_willmsg(WillMsg),
    {stop, Reason, State#session{will_msg = undefined, conn_pid = undefined}};

handle_info({'EXIT', ConnPid, _Reason}, State = #session{conn_pid = ConnPid}) ->
    State1 = ensure_will_delay_timer(State),
    {noreply, ensure_expire_timer(State1#session{conn_pid = undefined})};

handle_info({'EXIT', OldPid, _Reason}, State = #session{old_conn_pid = OldPid}) ->
    %% ignore
    {noreply, State#session{old_conn_pid = undefined}};

handle_info({'EXIT', Pid, Reason}, State = #session{conn_pid = ConnPid}) ->
    ?LOG(error, "Unexpected EXIT: conn_pid=~p, exit_pid=~p, reason=~p",
         [ConnPid, Pid, Reason]),
    {noreply, State};

handle_info(Info, State) ->
    emqx_logger:error("[Session] unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(Reason, #session{will_msg = WillMsg,
                         client_id = ClientId,
                         conn_pid = ConnPid,
                         old_conn_pid = OldConnPid}) ->
    send_willmsg(WillMsg),
    [maybe_shutdown(Pid, Reason) || Pid <- [ConnPid, OldConnPid]],
    emqx_hooks:run('session.terminated', [#{client_id => ClientId}, Reason]).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

maybe_shutdown(undefined, _Reason) ->
    ok;
maybe_shutdown(Pid, normal) ->
     Pid ! {shutdown, normal};
maybe_shutdown(Pid, Reason) ->
    exit(Pid, Reason).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

has_connection(#session{conn_pid = Pid}) ->
    is_pid(Pid) andalso is_process_alive(Pid).

handle_dispatch(Topic, Msg, State = #session{subscriptions = SubMap}) ->
    case maps:find(Topic, SubMap) of
        {ok, #{nl := Nl, qos := QoS, rap := Rap, subid := SubId}} ->
            run_dispatch_steps([{nl, Nl}, {qos, QoS}, {rap, Rap}, {subid, SubId}], Msg, State);
        {ok, #{nl := Nl, qos := QoS, rap := Rap}} ->
            run_dispatch_steps([{nl, Nl}, {qos, QoS}, {rap, Rap}], Msg, State);
        error ->
            dispatch(emqx_message:unset_flag(dup, Msg), State)
    end.

suback(_From, undefined, _ReasonCodes) ->
    ignore;
suback(From, PacketId, ReasonCodes) ->
    From ! {deliver, {suback, PacketId, ReasonCodes}}.


%%------------------------------------------------------------------------------
%% Kickout old connection

kick(_ClientId, undefined, _ConnPid) ->
    ignore;
kick(_ClientId, ConnPid, ConnPid) ->
    ignore;
kick(ClientId, OldConnPid, ConnPid) ->
    unlink(OldConnPid),
    OldConnPid ! {shutdown, conflict, {ClientId, ConnPid}},
    %% Clean noproc
    receive {'EXIT', OldConnPid, _} -> ok after 1 -> ok end.

%%------------------------------------------------------------------------------
%% Replay or Retry Delivery
%%------------------------------------------------------------------------------

%% Redeliver at once if force is true
retry_delivery(Force, State = #session{inflight = Inflight}) ->
    case emqx_inflight:is_empty(Inflight) of
        true  -> State;
        false ->
            SortFun = fun({_, _, Ts1}, {_, _, Ts2}) -> Ts1 < Ts2 end,
            Msgs = lists:sort(SortFun, emqx_inflight:values(Inflight)),
            retry_delivery(Force, Msgs, os:timestamp(), State)
    end.

retry_delivery(_Force, [], _Now, State) ->
    %% Retry again...
    ensure_retry_timer(State);

retry_delivery(Force, [{Type, Msg0, Ts} | Msgs], Now,
               State = #session{inflight = Inflight, retry_interval = Interval}) ->
    %% Microseconds -> MilliSeconds
    Age = timer:now_diff(Now, Ts) div 1000,
    if
        Force orelse (Age >= Interval) ->
            Inflight1 = case {Type, Msg0} of
                            {publish, {PacketId, Msg}} ->
                                case emqx_message:is_expired(Msg) of
                                    true ->
                                        emqx_metrics:trans(inc, 'messages/expired'),
                                        emqx_inflight:delete(PacketId, Inflight);
                                    false ->
                                        redeliver({PacketId, Msg}, State),
                                        emqx_inflight:update(PacketId, {publish, {PacketId, Msg}, Now}, Inflight)
                                end;
                            {pubrel, PacketId} ->
                                redeliver({pubrel, PacketId}, State),
                                emqx_inflight:update(PacketId, {pubrel, PacketId, Now}, Inflight)
                        end,
            retry_delivery(Force, Msgs, Now, State#session{inflight = Inflight1});
        true ->
            ensure_retry_timer(Interval - max(0, Age), State)
    end.

%%------------------------------------------------------------------------------
%% Send Will Message
%%------------------------------------------------------------------------------
send_willmsg(undefined) ->
    ignore;
send_willmsg(WillMsg) ->
    emqx_broker:publish(WillMsg).

%%------------------------------------------------------------------------------
%% Expire Awaiting Rel
%%------------------------------------------------------------------------------

expire_awaiting_rel(State = #session{awaiting_rel = AwaitingRel}) ->
    case maps:size(AwaitingRel) of
        0 -> State;
        _ -> expire_awaiting_rel(lists:keysort(2, maps:to_list(AwaitingRel)), os:timestamp(), State)
    end.

expire_awaiting_rel([], _Now, State) ->
    State#session{await_rel_timer = undefined};

expire_awaiting_rel([{PacketId, Ts} | More], Now,
                    State = #session{awaiting_rel = AwaitingRel, await_rel_timeout = Timeout}) ->
    case (timer:now_diff(Now, Ts) div 1000) of
        Age when Age >= Timeout ->
            emqx_metrics:trans(inc, 'messages/qos2/expired'),
            ?LOG(warning, "Dropped qos2 packet ~s for await_rel_timeout", [PacketId]),
            expire_awaiting_rel(More, Now, State#session{awaiting_rel = maps:remove(PacketId, AwaitingRel)});
        Age ->
            ensure_await_rel_timer(Timeout - max(0, Age), State)
    end.

%%------------------------------------------------------------------------------
%% Check awaiting rel
%%------------------------------------------------------------------------------

is_awaiting_full(#session{max_awaiting_rel = 0}) ->
    false;
is_awaiting_full(#session{awaiting_rel = AwaitingRel, max_awaiting_rel = MaxLen}) ->
    maps:size(AwaitingRel) >= MaxLen.

%%------------------------------------------------------------------------------
%% Dispatch Messages
%%------------------------------------------------------------------------------

run_dispatch_steps([], Msg, State) ->
    dispatch(Msg, State);
run_dispatch_steps([{nl, 1}|_Steps], #message{from = ClientId}, State = #session{client_id = ClientId}) ->
    State;
run_dispatch_steps([{nl, _}|Steps], Msg, State) ->
    run_dispatch_steps(Steps, Msg, State);
run_dispatch_steps([{qos, SubQoS}|Steps], Msg0 = #message{qos = PubQoS}, State = #session{upgrade_qos = false}) ->
    %% Ack immediately if a shared dispatch QoS is downgraded to 0
    Msg = case SubQoS =:= ?QOS_0 of
              true -> emqx_shared_sub:maybe_ack(Msg0);
              false -> Msg0
          end,
    run_dispatch_steps(Steps, Msg#message{qos = min(SubQoS, PubQoS)}, State);
run_dispatch_steps([{qos, SubQoS}|Steps], Msg = #message{qos = PubQoS}, State = #session{upgrade_qos = true}) ->
    run_dispatch_steps(Steps, Msg#message{qos = max(SubQoS, PubQoS)}, State);
run_dispatch_steps([{rap, _Rap}|Steps], Msg = #message{flags = Flags, headers = #{retained := true}}, State = #session{}) ->
    run_dispatch_steps(Steps, Msg#message{flags = maps:put(retain, true, Flags)}, State);
run_dispatch_steps([{rap, 0}|Steps], Msg = #message{flags = Flags}, State = #session{}) ->
    run_dispatch_steps(Steps, Msg#message{flags = maps:put(retain, false, Flags)}, State);
run_dispatch_steps([{rap, _}|Steps], Msg, State) ->
    run_dispatch_steps(Steps, Msg, State);
run_dispatch_steps([{subid, SubId}|Steps], Msg, State) ->
    run_dispatch_steps(Steps, emqx_message:set_header('Subscription-Identifier', SubId, Msg), State).

%% Enqueue message if the client has been disconnected
dispatch(Msg, State = #session{client_id = ClientId, conn_pid = undefined}) ->
    case emqx_hooks:run('message.dropped', [#{client_id => ClientId}, Msg]) of
        ok -> enqueue_msg(Msg, State);
        stop -> State
    end;

%% Deliver qos0 message directly to client
dispatch(Msg = #message{qos = ?QOS_0} = Msg, State) ->
    ok = deliver(undefined, Msg, State),
    State;

dispatch(Msg = #message{qos = QoS} = Msg,
         State = #session{next_pkt_id = PacketId, inflight = Inflight})
    when QoS =:= ?QOS_1 orelse QoS =:= ?QOS_2 ->
    case emqx_inflight:is_full(Inflight) of
        true ->
            enqueue_msg(Msg, State);
        false ->
            ok = deliver(PacketId, Msg, State),
            await(PacketId, Msg, next_pkt_id(State))
    end.

enqueue_msg(Msg, State = #session{mqueue = Q}) ->
    emqx_pd:update_counter(enqueue_stats, 1),
    {Dropped, NewQ} = emqx_mqueue:in(Msg, Q),
    Dropped =/= undefined andalso emqx_shared_sub:maybe_nack_dropped(Dropped),
    State#session{mqueue = NewQ}.

%%------------------------------------------------------------------------------
%% Deliver
%%------------------------------------------------------------------------------

redeliver({PacketId, Msg = #message{qos = QoS}}, State) ->
    deliver(PacketId, if QoS =:= ?QOS_2 -> Msg;
                         true -> emqx_message:set_flag(dup, Msg)
                      end, State);

redeliver({pubrel, PacketId}, #session{conn_pid = ConnPid}) ->
    ConnPid ! {deliver, {pubrel, PacketId}}.

deliver(PacketId, Msg, State) ->
    emqx_pd:update_counter(deliver_stats, 1),
    %% Ack QoS1/QoS2 messages when message is delivered to connection.
    %% NOTE: NOT to wait for PUBACK because:
    %% The sender is monitoring this session process,
    %% if the message is delivered to client but connection or session crashes,
    %% sender will try to dispatch the message to the next shared subscriber.
    %% This violates spec as QoS2 messages are not allowed to be sent to more
    %% than one member in the group.
    do_deliver(PacketId, emqx_shared_sub:maybe_ack(Msg), State).

do_deliver(PacketId, Msg, #session{conn_pid = ConnPid, binding = local}) ->
    ConnPid ! {deliver, {publish, PacketId, Msg}}, ok;
do_deliver(PacketId, Msg, #session{conn_pid = ConnPid, binding = remote}) ->
    emqx_rpc:cast(node(ConnPid), erlang, send, [ConnPid, {deliver, {publish, PacketId, Msg}}]).

%%------------------------------------------------------------------------------
%% Awaiting ACK for QoS1/QoS2 Messages
%%------------------------------------------------------------------------------

await(PacketId, Msg, State = #session{inflight = Inflight}) ->
    Inflight1 = emqx_inflight:insert(
                  PacketId, {publish, {PacketId, Msg}, os:timestamp()}, Inflight),
    ensure_retry_timer(State#session{inflight = Inflight1}).

acked(puback, PacketId, State = #session{client_id = ClientId, inflight  = Inflight}) ->
    case emqx_inflight:lookup(PacketId, Inflight) of
        {value, {publish, {_, Msg}, _Ts}} ->
            emqx_hooks:run('message.acked', [#{client_id => ClientId}], Msg),
            State#session{inflight = emqx_inflight:delete(PacketId, Inflight)};
        none ->
            ?LOG(warning, "Duplicated PUBACK PacketId ~w", [PacketId]),
            State
    end;

acked(pubrec, PacketId, State = #session{client_id = ClientId, inflight  = Inflight}) ->
    case emqx_inflight:lookup(PacketId, Inflight) of
        {value, {publish, {_, Msg}, _Ts}} ->
            emqx_hooks:run('message.acked', [#{client_id => ClientId}], Msg),
            State#session{inflight = emqx_inflight:update(PacketId, {pubrel, PacketId, os:timestamp()}, Inflight)};
        {value, {pubrel, PacketId, _Ts}} ->
            ?LOG(warning, "Duplicated PUBREC PacketId ~w", [PacketId]),
            State;
        none ->
            ?LOG(warning, "Unexpected PUBREC PacketId ~w", [PacketId]),
            State
    end;

acked(pubcomp, PacketId, State = #session{inflight = Inflight}) ->
    State#session{inflight = emqx_inflight:delete(PacketId, Inflight)}.

%%------------------------------------------------------------------------------
%% Dequeue
%%------------------------------------------------------------------------------

%% Do nothing if client is disconnected
dequeue(State = #session{conn_pid = undefined}) ->
    State;

dequeue(State = #session{inflight = Inflight}) ->
    case emqx_inflight:is_full(Inflight) of
        true  -> State;
        false -> dequeue2(State)
    end.

dequeue2(State = #session{mqueue = Q}) ->
    case emqx_mqueue:out(Q) of
        {empty, _Q} -> State;
        {{value, Msg}, Q1} ->
            %% Dequeue more
            dequeue(dispatch(Msg, State#session{mqueue = Q1}))
    end.

%%------------------------------------------------------------------------------
%% Ensure timers

ensure_await_rel_timer(State = #session{await_rel_timer = undefined, await_rel_timeout = Timeout}) ->
    ensure_await_rel_timer(Timeout, State);

ensure_await_rel_timer(State) ->
    State.

ensure_await_rel_timer(Timeout, State = #session{await_rel_timer = undefined}) ->
    State#session{await_rel_timer = emqx_misc:start_timer(Timeout, check_awaiting_rel)};
ensure_await_rel_timer(_Timeout, State) ->
    State.

ensure_retry_timer(State = #session{retry_timer = undefined, retry_interval = Interval}) ->
    ensure_retry_timer(Interval, State);
ensure_retry_timer(State) ->
    State.

ensure_retry_timer(Interval, State = #session{retry_timer = undefined}) ->
    State#session{retry_timer = emqx_misc:start_timer(Interval, retry_delivery)};
ensure_retry_timer(_Timeout, State) ->
    State.

ensure_expire_timer(State = #session{expiry_interval = Interval}) when Interval > 0 andalso Interval =/= 16#ffffffff ->
    State#session{expiry_timer = emqx_misc:start_timer(Interval * 1000, expired)};
ensure_expire_timer(State) ->
    State.

ensure_will_delay_timer(State = #session{will_msg = #message{headers = #{'Will-Delay-Interval' := WillDelayInterval}}}) ->
    State#session{will_delay_timer = emqx_misc:start_timer(WillDelayInterval * 1000, will_delay)};
ensure_will_delay_timer(State = #session{will_msg = WillMsg}) ->
    send_willmsg(WillMsg),
    State#session{will_msg = undefined}.

%% Take only the payload size into account, add other fields if necessary
msg_size(#message{payload = Payload}) -> payload_size(Payload).

%% Payload should be binary(), but not 100% sure. Need dialyzer!
payload_size(Payload) -> erlang:iolist_size(Payload).

%%------------------------------------------------------------------------------
%% Next Packet Id

next_pkt_id(State = #session{next_pkt_id = 16#FFFF}) ->
    State#session{next_pkt_id = 1};

next_pkt_id(State = #session{next_pkt_id = Id}) ->
    State#session{next_pkt_id = Id + 1}.

