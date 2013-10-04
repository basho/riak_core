%% -------------------------------------------------------------------
%%
%% Disterl socket buffer size monitor
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

-module(riak_core_dist_mon).

-export([start_link/0, set_dist_buf_sizes/2]).

%% gen_server callbacks
-behavior(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {}).

%% provided for testing convenience and debugging.
set_dist_buf_sizes(SendSz, RecvSz) 
  when (is_integer(SendSz) andalso SendSz > 0) andalso
       (is_integer(RecvSz) andalso RecvSz > 0) ->
    gen_server:call(?MODULE, {set_dist_buf_sizes, SendSz, RecvSz}).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
    %% register for monitor events so we can adjust buffers on 
    %% new connections when they're started.
    ok = net_kernel:monitor_nodes(true, [{node_type, visible}, nodedown_reason]),
    DistCtrl = erlang:system_info(dist_ctrl),
    %% make sure that buffers are correct on existing connections.
    [set_port_buffers(Port)
     || {_Node, Port} <- DistCtrl],
    {ok, #state{}}.



handle_call({set_dist_buf_sizes, SendSz, RecvSz}, _From, State) ->
    Reply = 
        [inet:setopts(Port, [{sndbuf, SendSz}, {recbuf, RecvSz}])
         || {_Node, Port} <- erlang:system_info(dist_ctrl)],
    {reply, Reply, State};
handle_call(Msg, _From, State) ->
    lager:warning("unknown call message received: ~p", [Msg]),
    {noreply, State}.

handle_cast(Msg, State) ->
    lager:warning("unknown cast message received: ~p", [Msg]),
    {noreply, State}.


handle_info({nodeup, Node, _InfoList}, State) ->
    DistCtrl = erlang:system_info(dist_ctrl),
    case proplists:get_value(Node, DistCtrl) of
        undefined ->
            lager:error("Could not get dist for ~p\n~p\n", [Node, DistCtrl]),
            {noreply, State};
        Port ->
            set_port_buffers(Port),
            {noreply, State}
    end;
handle_info({nodedown, _Node, _InfoList}, State) ->
    %% don't think we need to do anything here
    {noreply, State};
handle_info(Msg, State) ->
    lager:warning("unknown info message received: ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% private 

set_port_buffers(Port) ->
    {ok, SndBufSz} = application:get_env(riak_core, dist_send_buf_size),
    {ok, RecBufSz} = application:get_env(riak_core, dist_recv_buf_size),
    inet:setopts(Port, [{sndbuf, SndBufSz},
                        {recbuf, RecBufSz}]).
