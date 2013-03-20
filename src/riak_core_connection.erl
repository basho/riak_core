%% -------------------------------------------------------------------
%%
%% Riak Subprotocol Server Dispatch and Client Connections
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
-module(riak_core_connection).
-author("Chris Tilt").

-include("riak_core_connection.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(TRACE(Stmt),Stmt).
-else.
-define(TRACE(Stmt),ok).
-endif.


%% public API
-export([connect/2,
         sync_connect/2]).

%% internal functions
-export([async_connect_proc/3]).

%%TODO: move to riak_ring_core...symbolic cluster naming
-export([set_symbolic_clustername/2, set_symbolic_clustername/1,
         symbolic_clustername/1, symbolic_clustername/0]).

%% @doc Sets the symbolic, human readable, name of this cluster.
set_symbolic_clustername(Ring, ClusterName) ->
    {new_ring,
     riak_core_ring:update_meta(symbolic_clustername, ClusterName, Ring)}.

set_symbolic_clustername(ClusterName) when is_list(ClusterName) ->
    {error, "argument is not a string"};
set_symbolic_clustername(ClusterName) ->
    case riak_core_ring_manager:get_my_ring() of
        {ok, _Ring} ->
            riak_core_ring_manager:ring_trans(fun riak_core_connection:set_symbolic_clustername/2,
                                              ClusterName);
        {error, Reason} ->
            lager:error("Can't set symbolic clustername because: ~p", [Reason])
    end.

symbolic_clustername(Ring) ->
    case riak_core_ring:get_meta(symbolic_clustername, Ring) of
        {ok, Name} -> Name;
        undefined -> "undefined"
    end.

%% @doc Returns the symbolic, human readable, name of this cluster.
symbolic_clustername() ->
    case riak_core_ring_manager:get_my_ring() of
        {ok, Ring} ->
            symbolic_clustername(Ring);
        {error, Reason} ->
            lager:error("Can't read symbolic clustername because: ~p", [Reason]),
            "undefined"
    end.

%% Make async connection request. The connection manager is responsible for retry/backoff
%% and calls your module's functions on success or error (asynchrously):
%%   Module:connected(Socket, TransportModule, {IpAddress, Port}, {Proto, MyVer, RemoteVer}, Args)
%%   Module:connect_failed(Proto, {error, Reason}, Args)
%%       Reason could be 'protocol_version_not_supported"
%%
%%
%% You can set options on the tcp connection, e.g.
%% [{packet, 4}, {active, false}, {keepalive, true}, {nodelay, true}]
%%
%% ClientProtocol specifies the preferences of the client, in terms of what versions
%% of a protocol it speaks. The host will choose the highest common major version and
%% inform the client via the callback Module:connected() in the HostProtocol parameter.
%%
%% Note: that the connection will initially be setup with the `binary` option
%% because protocol negotiation requires binary. TODO: should we allow non-binary
%% options? Check that binary is not overwritten.
%%
%% connect returns the pid() of the asynchronous process that will attempt the connection.

-spec(connect(ip_addr(), clientspec()) -> pid()).
connect({IP,Port}, ClientSpec) ->
    ?TRACE(?debugMsg("spawning async_connect link")),
    %% start a process to handle the connection request asyncrhonously
    proc_lib:spawn_link(?MODULE, async_connect_proc, [self(), {IP,Port}, ClientSpec]).

sync_connect({IP,Port}, ClientSpec) ->
    sync_connect_status(self(), {IP,Port}, ClientSpec).

%% @private

%% exchange brief handshake with client to ensure that we're supporting sub-protocols.
%% client -> server : Hello {1,0} [Capabilities]
%% server -> client : Ack {1,0} [Capabilities]
exchange_handshakes_with(host, Socket, Transport, MyCaps) ->
    Hello = term_to_binary({?CTRL_HELLO, ?CTRL_REV, MyCaps}),
    case Transport:send(Socket, Hello) of
        ok ->
            ?TRACE(?debugFmt("exchange_handshakes: waiting for ~p from host", [?CTRL_ACK])),
            case Transport:recv(Socket, 0, ?CONNECTION_SETUP_TIMEOUT) of
                {ok, Ack} ->
                    case binary_to_term(Ack) of
                        {?CTRL_ACK, TheirRev, TheirCaps} ->
                            Props = [{local_revision, ?CTRL_REV}, {remote_revision, TheirRev} | TheirCaps],
                            {ok,Props};
                        {error, _Reason} = Error ->
                            Error;
                        Msg ->
                            {error, Msg}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        Error ->
            Error
    end.

async_connect_proc(Parent, {IP,Port}, ProtocolSpec) ->
    sync_connect_status(Parent, {IP,Port}, ProtocolSpec).

%% connect synchronously to remote addr/port and return status
sync_connect_status(_Parent, {IP,Port}, {ClientProtocol, {Options, Module, Args}}) ->
    Timeout = ?CONNECTION_SETUP_TIMEOUT,
    Transport = ranch_tcp,
    %%   connect to host's {IP,Port}
    ?TRACE(?debugFmt("sync_connect: connect to ~p", [{IP,Port}])),
    case gen_tcp:connect(IP, Port, ?CONNECT_OPTIONS, Timeout) of
        {ok, Socket} ->
            ?TRACE(?debugFmt("Setting system options on client side: ~p", [?CONNECT_OPTIONS])),
            Transport:setopts(Socket, ?CONNECT_OPTIONS),
            %% handshake to make sure it's a riak sub-protocol dispatcher
            MyName = symbolic_clustername(),
            MyCaps = [{clustername, MyName}],
            case exchange_handshakes_with(host, Socket, Transport, MyCaps) of
                {ok,Props} ->
                    %% ask for protocol, see what host has
                    case negotiate_proto_with_server(Socket, Transport, ClientProtocol) of
                        {ok,HostProtocol} ->
                            %% set client's requested Tcp options
                            ?TRACE(?debugFmt("Setting user options on client side; ~p", [Options])),
                            Transport:setopts(Socket, Options),
                            %% notify requester of connection and negotiated protocol from host
                            %% pass back returned value in case problem detected on connection
                            %% by module.  requestor is responsible for transferring control
                            %% of the socket.
                            Module:connected(Socket, Transport, {IP, Port}, HostProtocol, Args, Props);
                        {error, Reason} ->
                            ?TRACE(?debugFmt("negotiate_proto_with_server returned: ~p", [{error,Reason}])),
                            %% Module:connect_failed(ClientProtocol, {error, Reason}, Args),
                            {error, Reason}
                    end;
                {error, closed} ->
                    %% socket got closed, don't report this
                    {error, closed};
                {error, Reason} ->
                    lager:error("Failed to exchange handshake with host. Error = ~p", [Reason]),
                    {error, Reason};
                Error ->
                    %% failed to exchange handshakes
                    lager:error("Failed to exchange handshake with host. Error = ~p", [Error]),
                    {error, Error}
            end;
        {error, Reason} ->
            %% Module:connect_failed(ClientProtocol, {error, Reason}, Args),
            {error, Reason}
    end.

%% Negotiate the highest common major protocol revisision with the connected server.
%% client -> server : Prefs List = {SubProto, [{Major, Minor}]}
%% server -> client : selected version = {SubProto, {Major, HostMinor, ClientMinor}}
%%
%% returns {ok,{Proto,{Major,ClientMinor},{Major,HostMinor}}} | {error, Reason}
negotiate_proto_with_server(Socket, Transport, ClientProtocol) ->
    ?TRACE(?debugFmt("negotiate protocol with host, client proto = ~p", [ClientProtocol])),
    Transport:send(Socket, erlang:term_to_binary(ClientProtocol)),
    case Transport:recv(Socket, 0, ?CONNECTION_SETUP_TIMEOUT) of
        {ok, NegotiatedProtocolBin} ->
            case erlang:binary_to_term(NegotiatedProtocolBin) of
                {ok, {Proto,{CommonMajor,HMinor,CMinor}}} ->
                    {ok, {Proto,{CommonMajor,CMinor},{CommonMajor,HMinor}}};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            lager:error("Failed to receive protocol ~p response from server. Reason = ~p",
                        [ClientProtocol, Reason]),
            {error, connection_failed}
    end.
