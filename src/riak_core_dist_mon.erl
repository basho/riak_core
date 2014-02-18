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

-export([start_link/0, set_dist_buf_sizes/2, get_riak_env_vars/0]).

%% gen_server callbacks
-behavior(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          sndbuf :: non_neg_integer(),
          recbuf :: non_neg_integer()
         }).

%% provided for testing convenience and debugging.
set_dist_buf_sizes(SndBuf, RecBuf) 
  when (is_integer(SndBuf) andalso SndBuf > 0) andalso
       (is_integer(RecBuf) andalso RecBuf > 0) ->
    gen_server:call(?MODULE, {set_dist_buf_sizes, SndBuf, RecBuf}).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
    %% register for monitor events so we can adjust buffers on 
    %% new connections when they're started.
    ok = net_kernel:monitor_nodes(true, [{node_type, visible}, nodedown_reason]),
    {SndBuf, RecBuf} = get_riak_env_vars(),
    DistCtrl = erlang:system_info(dist_ctrl),
    %% make sure that buffers are correct on existing connections.
    _ = [set_port_buffers(Port, SndBuf, RecBuf)
     || {_Node, Port} <- DistCtrl],
    {ok, #state{sndbuf=SndBuf, recbuf=RecBuf}}.

get_riak_env_vars() ->
    {ok, SndBuf} = application:get_env(riak_core, dist_send_buf_size),
    {ok, RecBuf} = application:get_env(riak_core, dist_recv_buf_size),
    {SndBuf, RecBuf}.

handle_call({set_dist_buf_sizes, SndBuf, RecBuf}, _From, State) ->
    _ = [set_port_buffers(Port, SndBuf, RecBuf)
     || {_Node, Port} <- erlang:system_info(dist_ctrl)],
    {reply, ok, State#state{sndbuf=SndBuf, recbuf=RecBuf}};
handle_call(Msg, _From, State) ->
    lager:warning("unknown call message received: ~p", [Msg]),
    {noreply, State}.

handle_cast(Msg, State) ->
    lager:warning("unknown cast message received: ~p", [Msg]),
    {noreply, State}.


handle_info({nodeup, Node, _InfoList}, #state{sndbuf=SndBuf,
                                              recbuf=RecBuf} = State) ->
    DistCtrl = erlang:system_info(dist_ctrl),
    case proplists:get_value(Node, DistCtrl) of
        undefined ->
            lager:error("Could not get dist for ~p\n~p\n", [Node, DistCtrl]),
            {noreply, State};
        Port ->
            ok = set_port_buffers(Port, SndBuf, RecBuf),
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

set_port_buffers(Port, SndBuf, RecBuf) ->
    inet:setopts(Port, [{sndbuf, SndBuf},
                        {recbuf, RecBuf}]).
