%% -------------------------------------------------------------------
%%
%% Riak Subprotocol Server Dispatcher
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc Listens on a single TCP port and negotiates which protocol to start
%% on a new connection. Ranch is used to create a connection pool and accept
%% new socket connections. When a connection is accepted, the client supplies
%% a hello with thier revision and capabilities. The server replies in kind.
%% The client then sends the service they wish to use, and which versions of
%% the service they support. The server will find the highest major version in
%% common, and highest major version in common. If there is no major version in
%% common, the connectin fails. Minor versions do not need to match. On a
%% success, the server sends the Major version, Client minor version, and
%% Host minor version to the client. After that, the registered
%% module:function/5 is called and control of the socket passed to it.


-module(riak_core_service_mgr).
-author("Chris Tilt").
-behaviour(gen_server).

-include("riak_core_connection.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(TRACE(Stmt),Stmt).
%%-define(TRACE(Stmt),ok).
-else.
-define(TRACE(Stmt),ok).
-endif.

-define(SERVER, riak_core_service_manager).
-define(MAX_LISTENERS, 100).

%% services := registered protocols, key :: proto_id()
-record(state, {dispatch_addr = {"localhost", 9000} :: ip_addr(),
                services = orddict:new() :: orddict:orddict(),
                dispatcher_pid = undefined :: pid(),
                status_notifiers = [],
                service_stats = orddict:new() :: orddict:orddict(), % proto-id -> stats()
                refs = []
               }).

-export([start_link/0, start_link/1,
         register_service/2,
         unregister_service/1,
         is_registered/1,
         register_stats_fun/1,
         get_stats/0,
         stop/0
         ]).

%% ranch callbacks
-export([start_link/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% internal
-export([dispatch_service/4]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start the Service Manager on the default/configured Ip Address and Port.
%% All sub-protocols will be dispatched from there.
-spec(start_link() -> {ok, pid()}).
start_link() ->
    ServiceAddr = case app_helper:get_env(riak_core, cluster_mgr) of
        undefined ->
            lager:error("cluster_mgr is not configured for riak_core in
                app.config, defaulting to {\"127.0.0.1\", 0}."),
            {"127.0.0.1", 0};
        Res ->
            Res
    end,
    start_link(ServiceAddr).

%% @doc Start the Service Manager on the given Ip Address and Port.
%% All sub-protocols will be dispatched from there.
-spec(start_link(ip_addr()) -> {ok, pid()}).
start_link({IP,Port}) when is_integer(Port), Port >= 0 ->
    case valid_host_ip(IP) of
        false ->
            erlang:error({badarg, invalid_ip});
        true ->
            ?TRACE(?debugFmt("Starting Core Service Manager at ~p", [{IP,Port}])),
            lager:info("Starting Core Service Manager at ~p", [{IP,Port}]),
            Args = [{IP,Port}],
            Options = [],
            gen_server:start_link({local, ?SERVER}, ?MODULE, Args, Options)
    end.

%% @doc Once a protocol specification is registered, it will be kept available
%% by the Service Manager. Note that the callee is responsible for taking
%% ownership of the socket via Transport:controlling_process(Socket, Pid).
%% Only the strategy of `round_robin' is supported; it's arg is ignored.
-spec(register_service(hostspec(), service_scheduler_strategy()) -> ok).
register_service(HostProtocol, Strategy) ->
    %% only one strategy is supported as yet
    {round_robin, _NB} = Strategy,
    gen_server:cast(?SERVER, {register_service, HostProtocol, Strategy}).

%% @doc Unregister the given protocol-id. Existing connections for this
%% protocol are not killed. New connections for this protocol will not be
%% accepted until re-registered.
-spec(unregister_service(proto_id()) -> ok).
unregister_service(ProtocolId) ->
    gen_server:cast(?SERVER, {unregister_service, ProtocolId}).

%% @doc True if the given protocal id is registered.
-spec(is_registered(proto_id()) -> boolean()).
is_registered(ProtocolId) ->
    gen_server:call(?SERVER, {is_registered, service, ProtocolId}).

%% @doc Register a callback function that will get called periodically or
%% when the connection status of services changes. The function will
%% receive a list of tuples: {<protocol-id>, <stats>} where stats
%% holds the number of open connections that have been accepted  for that
%% protocol type. This can be used to report load, in the form of
%% connected-ness, for each protocol type, to remote clusters, e.g.,
%% making it possible for schedulers to balance the number of
%% connections across a cluster.
-spec register_stats_fun(Fun :: fun(([{proto_id(), non_neg_integer()}]) -> any())) -> 'ok'.
register_stats_fun(Fun) ->
    gen_server:cast(?SERVER, {register_stats_fun, Fun}).

%% @doc Number of open connections for each protocol id.
-spec get_stats() -> [{proto_id(), non_neg_integer()}].
get_stats() ->
    gen_server:call(?SERVER, get_stats).

%% @doc Stop the ranch listener, and then exit the server normally.
-spec stop() -> 'ok'.
stop() ->
    gen_server:call(?SERVER, stop).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([IpAddr]) ->
    {ok, Pid} = start_dispatcher(IpAddr, ?MAX_LISTENERS, []),
    {ok, #state{dispatch_addr = IpAddr, dispatcher_pid=Pid}}.

handle_call({is_registered, service, ProtocolId}, _From, State) ->
    Found = orddict:is_key(ProtocolId, State#state.services),
    {reply, Found, State};

handle_call(get_services, _From, State) ->
    {reply, orddict:to_list(State#state.services), State};

handle_call(stop, _From, State) ->
    ranch:stop_listener(State#state.dispatch_addr),
    {stop, normal, ok, State};

handle_call(get_stats, _From, State) ->
    Stats = orddict:to_list(State#state.service_stats),
    PStats = [{Protocol, Count} || {Protocol,{_Stats,Count}} <- Stats],
    {reply, PStats, State};

handle_call(_Unhandled, _From, State) ->
    ?TRACE(?debugFmt("Unhandled gen_server call: ~p", [_Unhandled])),
    {reply, {error, unhandled}, State}.

handle_cast({register_service, Protocol, Strategy}, State) ->
    {{ProtocolId,_Revs},_Rest} = Protocol,
    NewDict = orddict:store(ProtocolId, {Protocol, Strategy}, State#state.services),
    {noreply, State#state{services=NewDict}};
 
handle_cast({unregister_service, ProtocolId}, State) ->
    NewDict = orddict:erase(ProtocolId, State#state.services),
    {noreply, State#state{services=NewDict}};

handle_cast({register_stats_fun, Fun}, State) ->
    Notifiers = [Fun | State#state.status_notifiers],
    erlang:send_after(500, self(), status_update_timer),
    {noreply, State#state{status_notifiers=Notifiers}};

%% TODO: unregister support for notifiers?
%% handle_cast({unregister_node_status_fun, Fun}, State) ->
%%     Notifiers = [Fun | State#state.notifiers],
%%     {noreply, State#state{status_notifiers=Notifiers}};

handle_cast({service_up_event, Pid, ProtocolId}, State) ->
    ?TRACE(?debugFmt("Service up event: ~p", [ProtocolId])),
    erlang:send_after(500, self(), status_update_timer),
    Ref = erlang:monitor(process, Pid), %% arrange for us to receive 'DOWN' when Pid terminates
    ServiceStats = incr_count_for_protocol_id(ProtocolId, 1, State#state.service_stats),
    Refs = [{Ref,ProtocolId} | State#state.refs],
    {noreply, State#state{service_stats=ServiceStats, refs=Refs}};

handle_cast({service_down_event, _Pid, ProtocolId}, State) ->
    ?TRACE(?debugFmt("Service down event: ~p", [ProtocolId])),
    erlang:send_after(500, self(), status_update_timer),
    ServiceStats = incr_count_for_protocol_id(ProtocolId, -1, State#state.service_stats),
    {noreply, State#state{service_stats = ServiceStats}};

handle_cast(_Unhandled, _State) ->
    ?TRACE(?debugFmt("Unhandled gen_server cast: ~p", [_Unhandled])),
    {error, unhandled}. %% this will crash the server

handle_info(status_update_timer, State) ->
    %% notify all registered parties of this node's services counts
    Stats = orddict:to_list(State#state.service_stats),
    PStats = [ {Protocol, Count} || {Protocol,{_Stats,Count}} <- Stats],
    [NotifyFun(PStats) || NotifyFun <- State#state.status_notifiers],
    {noreply, State};

%% Get notified of a service that went down.
%% Remove it's Ref and pass the event on to get counted by ProtocolId
handle_info({'DOWN', Ref, process, Pid, _Reason}, State) ->
    Refs = State#state.refs,
    Refs2 = case lists:keytake(Ref, 1, Refs) of
                {value, {Ref, ProtocolId}, Rest} ->
                    gen_server:cast(?SERVER, {service_down_event, Pid, ProtocolId}),
                    Rest;
                error ->
                    Refs
            end,
    {noreply, State#state{refs=Refs2}};

handle_info(_Unhandled, State) ->
    ?TRACE(?debugFmt("Unhandled gen_server info: ~p", [_Unhandled])),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Private
%%%===================================================================

incr_count_for_protocol_id(ProtocolId, Incr, ServiceStatus) ->
    Stats2 = case orddict:find(ProtocolId, ServiceStatus) of
                 {ok, Stats} ->
                     Count = Stats#stats.open_connections,
                     Stats#stats{open_connections = Count + Incr};
                 error ->
                     #stats{open_connections = Incr}
             end,
    orddict:store(ProtocolId, Stats2, ServiceStatus).

%% @private
%% Host callback function, called by ranch for each accepted connection by way of
%% of the ranch:start_listener() call above, specifying this module.
start_link(Listener, Socket, Transport, SubProtocols) ->
    ?TRACE(?debugMsg("Start_link dispatch_service")),
    {ok, spawn_link(?MODULE, dispatch_service, [Listener, Socket, Transport, SubProtocols])}.

%% Body of the main dispatch loop. This is instantiated once for each connection
%% we accept because it transforms itself into the SubProtocol once it receives
%% the sub protocol and version, negotiated with the client.
dispatch_service(Listener, Socket, Transport, _Args) ->
    %% tell ranch "we've got it. thanks pardner"
    ok = ranch:accept_ack(Listener),
    %% set some starting options for the channel; these should match the client
    ?TRACE(?debugFmt("setting system options on service side: ~p", [?CONNECT_OPTIONS])),
    ok = Transport:setopts(Socket, ?CONNECT_OPTIONS),
    %% Version 1.0 capabilities just passes our clustername
    MyName = riak_core_connection:symbolic_clustername(),
    MyCaps = [{clustername, MyName}],
    case exchange_handshakes_with(client, Socket, Transport, MyCaps) of
        {ok,Props} ->
            %% get latest set of registered services from gen_server and do negotiation
            Services = gen_server:call(?SERVER, get_services),
            SubProtocols = [Protocol || {_Key,{Protocol,_Strategy}} <- Services],
            ?TRACE(?debugFmt("started dispatch_service with protocols: ~p",
                             [SubProtocols])),
            Negotiated = negotiate_proto_with_client(Socket, Transport, SubProtocols),
            ?TRACE(?debugFmt("negotiated = ~p", [Negotiated])),
            start_negotiated_service(Socket, Transport, Negotiated, Props);
        Error ->
            Error
    end.

%% start user's module:function and transfer socket to it's process.
start_negotiated_service(_Socket, _Transport, {error, Reason}, _Props) ->
    ?TRACE(?debugFmt("service dispatch failed with ~p", [{error, Reason}])),
    lager:error("service dispatch failed with ~p", [{error, Reason}]),
    {error, Reason};
%% Note that the callee is responsible for taking ownership of the socket via
%% Transport:controlling_process(Socket, Pid),
start_negotiated_service(Socket, Transport,
                         {NegotiatedProtocols, {Options, Module, Function, Args}},
                         Props) ->
    %% Set requested Tcp socket options now that we've finished handshake phase
    ?TRACE(?debugFmt("Setting user options on service side; ~p", [Options])),
    ?TRACE(?debugFmt("negotiated protocols: ~p", [NegotiatedProtocols])),
    Transport:setopts(Socket, Options),
    %% call service body function for matching protocol. The callee should start
    %% a process or gen_server or such, and return {ok, pid()}.
    case Module:Function(Socket, Transport, NegotiatedProtocols, Args, Props) of
        {ok, Pid} ->
            {ok,{ClientProto,_Client,_Host}} = NegotiatedProtocols,
            gen_server:cast(?SERVER, {service_up_event, Pid, ClientProto}),
            {ok, Pid};
        Error ->
            ?TRACE(?debugFmt("service dispatch of ~p:~p failed with ~p",
                             [Module, Function, Error])),
            lager:error("service dispatch of ~p:~p failed with ~p",
                        [Module, Function, Error]),
            Error
    end.

%% Negotiate the highest common major protocol revisision with the connected client.
%% client -> server : Prefs List = {SubProto, [{Major, Minor}]} as binary
%% server -> client : selected version = {SubProto, {Major, HostMinor}} as binary
%%
%% returns {ok,{{Proto,MyVer,RemoteVer},Options,Module,Function,Args}} | Error
negotiate_proto_with_client(Socket, Transport, HostProtocols) ->
    case Transport:recv(Socket, 0, ?CONNECTION_SETUP_TIMEOUT) of
        {ok, PrefsBin} ->
            {ClientProto,Versions} = erlang:binary_to_term(PrefsBin),
            case choose_version({ClientProto,Versions}, HostProtocols) of
                {error, Reason} ->
                    lager:error("Failed to negotiate protocol ~p from client because ~p",
                                [ClientProto, Reason]),
                    Transport:send(Socket, erlang:term_to_binary({error,Reason})),
                    {error, Reason};
                {ok,{ClientProto,Major,CN,HN}, Rest} ->
                    Transport:send(Socket, erlang:term_to_binary({ok,{ClientProto,{Major,HN,CN}}})),
                    {{ok,{ClientProto,{Major,HN},{Major,CN}}}, Rest};
                {error, Reason, Rest} ->
                    lager:error("Failed to negotiate protocol ~p from client because ~p",
                                [ClientProto, Reason]),
                    %% notify client it failed to negotiate
                    Transport:send(Socket, erlang:term_to_binary({error,Reason})),
                    {{error, Reason}, Rest}
            end;
        {error, Reason} ->
            lager:error("Failed to receive protocol request from client. Error = ~p",
                        [Reason]),
            {error, connection_failed}
    end.

choose_version({ClientProto,ClientVersions}=_CProtocol, HostProtocols) ->
    ?TRACE(?debugFmt("choose_version: client proto = ~p, HostProtocols = ~p",
                     [_CProtocol, HostProtocols])),
    %% first, see if the host supports the subprotocol
    case [H || {{HostProto,_Versions},_Rest}=H <- HostProtocols, ClientProto == HostProto] of
        [] ->
            %% oops! The host does not support this sub protocol type
            lager:error("Failed to find host support for protocol: ~p", [ClientProto]),
            ?TRACE(?debugMsg("choose_version: no common protocols")),
            {error,protocol_not_supported};
        [{{_HostProto,HostVersions},Rest}=_Matched | _DuplicatesIgnored] ->
            ?TRACE(?debugFmt("choose_version: unsorted = ~p clientversions = ~p",
                             [_Matched, ClientVersions])),
            CommonVers = [{CM,CN,HN} || {CM,CN} <- ClientVersions, {HM,HN} <- HostVersions, CM == HM],
            ?TRACE(?debugFmt("common versions = ~p", [CommonVers])),
            %% sort by major version, highest to lowest, and grab the top one.
            case lists:reverse(lists:keysort(1,CommonVers)) of
                [] ->
                    %% oops! No common major versions for Proto.
                    ?TRACE(?debugFmt("Failed to find a common major version for protocol: ~p",
                                     [ClientProto])),
                    lager:error("Failed to find a common major version for protocol: ~p", [ClientProto]),
                    {error,protocol_version_not_supported,Rest};
                [{Major,CN,HN}] ->
                    {ok, {ClientProto,Major,CN,HN},Rest};
                [{Major,CN,HN} | _] ->
                    {ok, {ClientProto,Major,CN,HN},Rest}
            end
    end.

%% exchange brief handshake with client to ensure that we're supporting sub-protocols.
%% client -> server : Hello {1,0} [Capabilities]
%% server -> client : Ack {1,0} [Capabilities]
exchange_handshakes_with(client, Socket, Transport, MyCaps) ->
    ?TRACE(?debugFmt("exchange_handshakes: waiting for ~p from client", [?CTRL_HELLO])),
    case Transport:recv(Socket, 0, ?CONNECTION_SETUP_TIMEOUT) of
        {ok, Hello} ->
            %% read their hello
            case binary_to_term(Hello) of
                {?CTRL_HELLO, TheirRev, TheirCaps} ->
                    Ack = term_to_binary({?CTRL_ACK, ?CTRL_REV, MyCaps}),
                    Transport:send(Socket, Ack),
                    %% make some props to hand dispatched service
                    Props = [{local_revision, ?CTRL_REV}, {remote_revision, TheirRev} | TheirCaps],
                    {ok,Props};
                Msg ->
                    %% tell other side we are failing them
                    Error = {error, bad_handshake},
                    Transport:send(Socket, erlang:term_to_binary(Error)),
                    lager:error("Control protocol handshake with client got unexpected hello: ~p",
                                [Msg]),
                    Error
            end;
        {error, Reason} ->
            lager:error("Failed to exchange handshake with client. Error = ~p", [Reason]),
            {error, Reason}
    end.

%% Returns true if the IP address given is a valid host IP address.
valid_host_ip("0.0.0.0") ->
    true;
valid_host_ip(IP) ->     
    {ok, IFs} = inet:getifaddrs(),
    {ok, NormIP} = normalize_ip(IP),
    lists:foldl(
        fun({_IF, Attrs}, Match) ->
                case lists:member({addr, NormIP}, Attrs) of
                    true ->
                        true;
                    _ ->
                        Match
                end
        end, false, IFs).

%% Convert IP address the tuple form
normalize_ip(IP) when is_list(IP) ->
    inet_parse:address(IP);
normalize_ip(IP) when is_tuple(IP) ->
    {ok, IP}.

%% @doc Start the connection dispatcher with a limit of MaxListeners
%% listener connections and supported sub-protocols. When a connection
%% request arrives, it is mapped via the associated Protocol atom to an
%% acceptor function called as Module:Function(Listener, Socket, Transport, Args),
%% which must create it's own process and return {ok, pid()}

-spec(start_dispatcher(ip_addr(), non_neg_integer(), [hostspec()]) -> {ok, pid()}).
start_dispatcher({IP,Port}, MaxListeners, SubProtocols) ->
    {ok, RawAddress} = inet_parse:address(IP),
    {ok, Pid} = ranch:start_listener({IP,Port}, MaxListeners, ranch_tcp,
                                [{ip, RawAddress}, {port, Port}],
                                ?MODULE, SubProtocols),
    lager:info("Service manager: listening on ~s:~p", [IP, Port]),
    {ok, Pid}.
