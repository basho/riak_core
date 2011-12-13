%% -------------------------------------------------------------------
%%
%% mock_vnode: mock vnode for unit testing
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc mock vnode for unit testing

-module(mock_vnode).
%TODO: Work out why this gives a warning
%-behavior(riak_core_vnode).
-export([start_vnode/1,
         get_index/1,
         get_counter/1,
         neverreply/1,
         returnreply/1,
         latereply/1,
         asyncnoreply/2,
         asyncreply/2,
         asynccrash/2,
         crash/1,
         get_crash_reason/1,
         stop/1]).
-export([init/1,
         handle_command/3,
         terminate/2,
         handle_exit/3]).
-export([init_worker/3,
         handle_work/3]).
-behavior(riak_core_vnode_worker).

-record(state, {index, counter, crash_reason}).
-record(wstate, {index, args, props, counter, crash_reason}).

-define(MASTER, mock_vnode_master).

%% API
start_vnode(I) ->
    riak_core_vnode_master:start_vnode(I, ?MODULE).

get_index(Preflist) ->
    riak_core_vnode_master:sync_command(Preflist, get_index, ?MASTER).

get_counter(Preflist) ->
    riak_core_vnode_master:sync_command(Preflist, get_counter, ?MASTER).

neverreply(Preflist) ->
    riak_core_vnode_master:command(Preflist, neverreply, ?MASTER).

returnreply(Preflist) ->
    Ref = {neverreply, make_ref()},
    riak_core_vnode_master:command(Preflist, returnreply, {raw, Ref, self()}, ?MASTER),
    {ok, Ref}.

latereply(Preflist) ->
    Ref = {latereply, make_ref()},
    riak_core_vnode_master:command(Preflist, latereply, {raw, Ref, self()}, ?MASTER),
    {ok, Ref}.

asyncnoreply(Preflist, AsyncDonePid) ->
    Ref = {asyncnoreply, make_ref()},
    riak_core_vnode_master:command(Preflist, {asyncnoreply, AsyncDonePid}, 
                                   {raw, Ref, AsyncDonePid}, ?MASTER),
    {ok, Ref}.

asyncreply(Preflist, AsyncDonePid) ->
    Ref = {asyncreply, make_ref()},
    riak_core_vnode_master:command(Preflist, {asyncreply, AsyncDonePid},
                                   {raw, Ref, AsyncDonePid}, ?MASTER),
    {ok, Ref}.

asynccrash(Preflist, AsyncDonePid) ->
    Ref = {asyncreply, make_ref()},
    riak_core_vnode_master:command(Preflist, {asynccrash, AsyncDonePid},
                                   {raw, Ref, AsyncDonePid}, ?MASTER),
    {ok, Ref}.

crash(Preflist) ->
    riak_core_vnode_master:sync_command(Preflist, crash, ?MASTER).

get_crash_reason(Preflist) ->
    riak_core_vnode_master:sync_command(Preflist, get_crash_reason, ?MASTER).

stop(Preflist) ->
    riak_core_vnode_master:sync_command(Preflist, stop, ?MASTER).


%% Callbacks

init([Index]) ->
    S = #state{index=Index,counter=0},
    {ok, PoolSize} = application:get_env(riak_core, core_vnode_eqc_pool_size),
    case PoolSize of
        PoolSize when PoolSize > 0 ->
            {ok, S, [{pool, ?MODULE, PoolSize, myargs}]};
        _ ->
            {ok, S}
    end.    

handle_command(get_index, _Sender, State) ->
    {reply, {ok, State#state.index}, State};
handle_command(get_counter, _Sender, State) ->
    {reply, {ok, State#state.counter}, State};
handle_command(get_crash_reason, _Sender, State) ->
    {reply, {ok, State#state.crash_reason}, State};

handle_command(crash, _Sender, State) ->
    spawn_link(fun() -> exit(State#state.index) end),
    {reply, ok, State};
handle_command(stop, Sender, State = #state{counter=Counter}) ->
    %% Send reply here as vnode_master:sync_command does a call 
    %% which is cast on to the vnode process.  Returning {stop,...}
    %% does not provide for returning a response.
    riak_core_vnode:reply(Sender, stopped),
    {stop, normal, State#state{counter = Counter + 1}};
handle_command(neverreply, _Sender, State = #state{counter=Counter}) ->
    {noreply, State#state{counter = Counter + 1}};
handle_command(returnreply, _Sender, State = #state{counter=Counter}) ->
    {reply, returnreply, State#state{counter = Counter + 1}};
handle_command(latereply, Sender, State = #state{counter=Counter}) ->
    spawn(fun() -> 
                  timer:sleep(1),
                  riak_core_vnode:reply(Sender, latereply)
          end),
    {noreply, State#state{counter = Counter + 1}};
handle_command({asyncnoreply, DonePid}, Sender, State = #state{counter=Counter}) ->
    {async, {noreply, DonePid}, Sender, State#state{counter = Counter + 1}};
handle_command({asyncreply, DonePid}, Sender, State = #state{counter=Counter}) ->
    {async, {reply, DonePid}, Sender, State#state{counter = Counter + 1}};
handle_command({asynccrash, DonePid}, Sender, State = #state{counter=Counter}) ->
    {async, {crash, DonePid}, Sender, State#state{counter = Counter + 1}}.

handle_exit(_Pid, Reason, State) ->
    {noreply, State#state{crash_reason=Reason}}.

terminate(_Reason, _State) ->
    ok.



%%
%% Vnode worker callbacks
%%
init_worker(Index, Args, Props) ->
    {ok, #wstate{index=Index, args=Args, props=Props}}.

handle_work({noreply, DonePid},  {raw, Ref, _EqcPid} = _Sender, State = #wstate{index=I}) ->
    timer:sleep(1), % slow things down enough to cause issue on stops
    DonePid ! {I, {ok, Ref}},
    {noreply, State};
handle_work({reply, DonePid},  {raw, Ref, _EqcPid} = _Sender, State = #wstate{index=I}) ->
    timer:sleep(1), % slow things down enough to cause issue on stops
    DonePid ! {I, {ok, Ref}},
    {reply, asyncreply, State};
handle_work({crash, DonePid},  {raw, Ref, _EqcPid} = _Sender, _State = #wstate{index=I}) ->
    timer:sleep(1), % slow things down enough to cause issue on stops
    %DonePid ! {I, {ok, Ref}},
    throw(deliberate_async_crash).

