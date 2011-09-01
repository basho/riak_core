%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
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

%% @doc the local view of the cluster's ring configuration

-module(riak_core_ring_manager).
-include_lib("eunit/include/eunit.hrl").
-define(RING_KEY, riak_ring).
-behaviour(gen_server2).

-export([start_link/0,
         start_link/1,
         get_my_ring/0,
         refresh_my_ring/0,
         refresh_ring/2,
         set_my_ring/1,
         write_ringfile/0,
         prune_ringfiles/0,
         read_ringfile/1,
         find_latest_ringfile/0,
         do_write_ringfile/1,
         ring_trans/2,
         set_cluster_name/1,
         stop/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-ifdef(TEST).
-export([set_ring_global/1]).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% ===================================================================
%% Public API
%% ===================================================================

start_link() ->
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [live], []).


%% Testing entry point
start_link(test) ->
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [test], []).


%% @spec get_my_ring() -> {ok, riak_core_ring:riak_core_ring()} | {error, Reason}
get_my_ring() ->
    case mochiglobal:get(?RING_KEY) of
        Ring when is_tuple(Ring) -> {ok, Ring};
        undefined -> {error, no_ring}
    end.

%% @spec refresh_my_ring() -> ok
refresh_my_ring() ->
    gen_server2:call(?MODULE, refresh_my_ring, infinity).

refresh_ring(Node, ClusterName) ->
    gen_server2:cast({?MODULE, Node}, {refresh_my_ring, ClusterName}).

%% @spec set_my_ring(riak_core_ring:riak_core_ring()) -> ok
set_my_ring(Ring) ->
    gen_server2:call(?MODULE, {set_my_ring, Ring}, infinity).


%% @spec write_ringfile() -> ok
write_ringfile() ->
    gen_server2:cast(?MODULE, write_ringfile).

ring_trans(Fun, Args) ->
    gen_server2:call(?MODULE, {ring_trans, Fun, Args}, infinity).

set_cluster_name(Name) ->
    gen_server2:call(?MODULE, {set_cluster_name, Name}, infinity).

do_write_ringfile(Ring) ->
    {{Year, Month, Day},{Hour, Minute, Second}} = calendar:universal_time(),
    TS = io_lib:format(".~B~2.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B",
                       [Year, Month, Day, Hour, Minute, Second]),
    case app_helper:get_env(riak_core, ring_state_dir) of
        "<nostore>" -> nop;
        Dir ->
            Cluster = app_helper:get_env(riak_core, cluster_name),
            FN = Dir ++ "/riak_core_ring." ++ Cluster ++ TS,
            ok = filelib:ensure_dir(FN),
            ok = file:write_file(FN, term_to_binary(Ring))
    end.

%% @spec find_latest_ringfile() -> string()
find_latest_ringfile() ->
    Dir = app_helper:get_env(riak_core, ring_state_dir),
    case file:list_dir(Dir) of
        {ok, Filenames} ->
            Cluster = app_helper:get_env(riak_core, cluster_name),
            Timestamps = [list_to_integer(TS) || {"riak_core_ring", C1, TS} <-
                                                     [list_to_tuple(string:tokens(FN, ".")) || FN <- Filenames],
                                                 C1 =:= Cluster],
            SortedTimestamps = lists:reverse(lists:sort(Timestamps)),
            case SortedTimestamps of
                [Latest | _] ->
                    {ok, Dir ++ "/riak_core_ring." ++ Cluster ++ "." ++ integer_to_list(Latest)};
                _ ->
                    {error, not_found}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @spec read_ringfile(string()) -> riak_core_ring:riak_core_ring() | {error, any()}
read_ringfile(RingFile) ->
    case file:read_file(RingFile) of
        {ok, Binary} ->
            binary_to_term(Binary);
        {error, Reason} -> {error, Reason}
    end.

%% @spec prune_ringfiles() -> ok | {error, Reason}
prune_ringfiles() ->
    case app_helper:get_env(riak_core, ring_state_dir) of
        "<nostore>" -> ok;
        Dir ->
            Cluster = app_helper:get_env(riak_core, cluster_name),
            case file:list_dir(Dir) of
                {error,enoent} -> ok;
                {error, Reason} ->
                    {error, Reason};
                {ok, []} -> ok;
                {ok, Filenames} ->
                    Timestamps = [TS || {"riak_core_ring", C1, TS} <- 
                     [list_to_tuple(string:tokens(FN, ".")) || FN <- Filenames],
                                        C1 =:= Cluster],
                    if Timestamps /= [] ->
                            %% there are existing ring files
                            TSPat = [io_lib:fread("~4d~2d~2d~2d~2d~2d",TS) ||
                                        TS <- Timestamps],
                            TSL = lists:reverse(lists:sort([TS ||
                                                               {ok,TS,[]} <- TSPat])),
                            Keep = prune_list(TSL),
                            KeepTSs = [lists:flatten(
                                         io_lib:format(
                                           "~B~2.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B",K))
                                       || K <- Keep],
                            DelFNs = [Dir ++ "/" ++ FN || FN <- Filenames, 
                                                          lists:all(fun(TS) -> 
                                                                            string:str(FN,TS)=:=0
                                                                    end, KeepTSs)],
                            [file:delete(DelFN) || DelFN <- DelFNs],
                            ok;
                       true ->
                            %% directory wasn't empty, but there are no ring
                            %% files in it
                            ok
                    end
            end
    end.


%% @private (only used for test instances)
stop() ->
    gen_server2:cast(?MODULE, stop).


%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([Mode]) ->
    case Mode of
        live ->
            Ring = riak_core_ring:fresh();
        test ->
            Ring = riak_core_ring:fresh(16,node())
    end,

    %% Set the ring and send initial notification to local observers that
    %% ring has changed.
    %% Do *not* save the ring to disk here.  On startup we deliberately come
    %% up with a ring where the local node owns all partitions so that any
    %% fallback vnodes will be started so they can hand off.
    set_ring_global(Ring),
    riak_core_ring_events:ring_update(Ring),
    {ok, Mode}.


handle_call({set_my_ring, RingIn}, _From, State) ->
    Ring = riak_core_ring:upgrade(RingIn),
    prune_write_notify_ring(Ring),
    {reply,ok,State};
handle_call(refresh_my_ring, _From, State) ->
    %% This node is leaving the cluster so create a fresh ring file
    FreshRing = riak_core_ring:fresh(),
    set_ring_global(FreshRing),
    %% Make sure the fresh ring gets written before stopping
    do_write_ringfile(FreshRing),

    %% Handoff is complete and fresh ring is written
    %% so we can safely stop now.
    riak_core:stop("node removal completed, exiting."),

    {reply,ok,State};
handle_call({ring_trans, Fun, Args}, _From, State) ->
    {ok, Ring} = get_my_ring(),
    case catch Fun(Ring, Args) of
        {new_ring, NewRing} ->
            prune_write_notify_ring(NewRing),
            riak_core_gossip:random_recursive_gossip(NewRing),
            {reply, {ok, NewRing}, State};
        {reconciled_ring, NewRing} ->
            prune_write_notify_ring(NewRing),
            riak_core_gossip:recursive_gossip(NewRing),
            {reply, {ok, NewRing}, State};
        ignore ->
            {reply, not_changed, State};
        Other ->
            lager:error("ring_trans: invalid return value: ~p", 
                                   [Other]),
            {reply, not_changed, State}
    end;
handle_call({set_cluster_name, Name}, _From, State) ->
    {ok, Ring} = get_my_ring(),
    NewRing = riak_core_ring:set_cluster_name(Ring, Name),
    prune_write_notify_ring(NewRing),
    {reply, ok, State}.

handle_cast(stop, State) ->
    {stop,normal,State};

handle_cast({refresh_my_ring, ClusterName}, State) ->
    {ok, Ring} = get_my_ring(),
    case riak_core_ring:cluster_name(Ring) of
        ClusterName ->
            handle_cast(refresh_my_ring, State);
        _ ->
            {noreply, ok, State}
    end;
handle_cast(refresh_my_ring, State) ->
    {_, _, State2} = handle_call(refresh_my_ring, undefined, State),
    {noreply, State2};

handle_cast(write_ringfile, test) ->
    {noreply,test};

handle_cast(write_ringfile, State) ->
    {ok, Ring} = get_my_ring(),
    do_write_ringfile(Ring),
    {noreply,State}.


handle_info(_Info, State) ->
    {noreply, State}.


%% @private
terminate(_Reason, _State) ->
    ok.


%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ===================================================================
%% Internal functions
%% ===================================================================

prune_list([X|Rest]) ->
    lists:usort(lists:append([[X],back(1,X,Rest),back(2,X,Rest),
                  back(3,X,Rest),back(4,X,Rest),back(5,X,Rest)])).
back(_N,_X,[]) -> [];
back(N,X,[H|T]) ->
    case lists:nth(N,X) =:= lists:nth(N,H) of
        true -> back(N,X,T);
        false -> [H]
    end.

%% Set the ring in mochiglobal.  Exported during unit testing
%% to make test setup simpler - no need to spin up a riak_core_ring_manager
%% process.
set_ring_global(Ring) ->
    mochiglobal:put(?RING_KEY, Ring).

%% Persist a new ring file, set the global value and notify any listeners
prune_write_notify_ring(Ring) ->
    riak_core_ring_manager:prune_ringfiles(),
    do_write_ringfile(Ring),
    set_ring_global(Ring),
    riak_core_ring_events:ring_update(Ring).

%% ===================================================================
%% Unit tests
%% ===================================================================
-ifdef(TEST).

back_test() ->
    X = [1,2,3],
    List1 = [[1,2,3],[4,2,3], [7,8,3], [11,12,13], [1,2,3]],
    List2 = [[7,8,9], [1,2,3]],
    List3 = [[1,2,3]],
    ?assertEqual([[4,2,3]], back(1, X, List1)),
    ?assertEqual([[7,8,9]], back(1, X, List2)),
    ?assertEqual([], back(1, X, List3)),
    ?assertEqual([[7,8,3]], back(2, X, List1)),
    ?assertEqual([[11,12,13]], back(3, X, List1)).    

prune_list_test() ->
    TSList1 = [[2011,2,28,16,32,16],[2011,2,28,16,32,36],[2011,2,28,16,30,27],[2011,2,28,16,32,16],[2011,2,28,16,32,36]],
    TSList2 = [[2011,2,28,16,32,36],[2011,2,28,16,31,16],[2011,2,28,16,30,27],[2011,2,28,16,32,16],[2011,2,28,16,32,36]],
    PrunedList1 = [[2011,2,28,16,30,27],[2011,2,28,16,32,16]],
    PrunedList2 = [[2011,2,28,16,31,16],[2011,2,28,16,32,36]],
    ?assertEqual(PrunedList1, prune_list(TSList1)),
    ?assertEqual(PrunedList2, prune_list(TSList2)).    

set_ring_global_test() ->
    application:set_env(riak_core,ring_creation_size, 4),
    Ring = riak_core_ring:fresh(),
    set_ring_global(Ring),
    ?assertEqual(Ring, mochiglobal:get(?RING_KEY)).

set_my_ring_test() ->
    application:set_env(riak_core,ring_creation_size, 4),
    Ring = riak_core_ring:fresh(),
    set_ring_global(Ring),
    ?assertEqual({ok, Ring}, get_my_ring()).

refresh_my_ring_test() ->
    application:set_env(riak_core, ring_creation_size, 4),
    application:set_env(riak_core, ring_state_dir, "/tmp"),
    application:set_env(riak_core, cluster_name, "test"),
    riak_core_ring_events:start_link(),
    riak_core_ring_manager:start_link(test),
    riak_core_vnode_sup:start_link(),
    riak_core_vnode_master:start_link(riak_core_vnode),
    riak_core_test_util:setup_mockring1(),
    ?assertEqual(ok, riak_core_ring_manager:refresh_my_ring()),
    riak_core_ring_manager:stop(),
    %% Cleanup the ring file created for this test
    {ok, RingFile} = find_latest_ringfile(),
    file:delete(RingFile).

-endif.

