%% -------------------------------------------------------------------
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
-module(riak_core_metadata_exchange_fsm).

-behaviour(gen_fsm).

%% API
-export([start/2]).

%% gen_fsm callbacks
-export([init/1, handle_event/3, handle_sync_event/4,
         handle_info/3, terminate/3, code_change/4]).

%% gen_fsm states
-export([prepare/2,
         prepare/3,
         update/2,
         update/3,
         exchange/2,
         exchange/3]).

-define(SERVER, ?MODULE).

-record(state, {
          %% node the exchange is taking place with
          peer    :: node(),

          %% count of trees that have been buit
          built   :: non_neg_integer(),

          %% length of time waited to aqcuire remote lock or
          %% update trees
          timeout :: pos_integer()
         }).

-record(exchange, {
          %% number of local prefixes repaired
          local   :: non_neg_integer(),

          %% number of remote prefixes repaired
          remote  :: non_neg_integer(),

          %% number of keys (missing, local, different) repaired,
          %% excluding those in prefixes counted by local/remote
          keys    :: non_neg_integer()
         }).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start an exchange of Cluster Metadata hashtrees between this node
%% and `Peer'. `Timeout' is the number of milliseconds the process will wait
%% to aqcuire the remote lock or to upate both trees.
-spec start(node(), pos_integer()) -> {ok, pid()} | ignore | {error, term()}.
start(Peer, Timeout) ->
    gen_fsm:start(?MODULE, [Peer, Timeout], []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init([Peer, Timeout]) ->
    gen_fsm:send_event(self(), start),
    {ok, prepare, #state{peer=Peer,built=0,timeout=Timeout}}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% gen_fsm states
%%%===================================================================
prepare(start, State) ->
    %% get local lock
    case riak_core_metadata_hashtree:lock() of
        ok ->
            %% get remote lock
            remote_lock_request(State#state.peer),
            {next_state, prepare, State, State#state.timeout};
        _Error ->
            {stop, normal, State}
    end;
prepare(timeout, State=#state{peer=Peer}) ->
    %% getting remote lock timed out
    lager:error("metadata exchange with ~p timed out aquiring locks", [Peer]),
    {stop, normal, State};
prepare({remote_lock, ok}, State) ->
    %% getting remote lock succeeded
    update(start, State);
prepare({remote_lock, _Error}, State) ->
    %% failed to get remote lock
    {stop, normal, State}.

update(start, State) ->
    update_request(node()),
    update_request(State#state.peer),
    {next_state, update, State, State#state.timeout};
update(timeout, State=#state{peer=Peer}) ->
    lager:error("metadata exchange with ~p timed out updating trees", [Peer]),
    {stop, normal, State};
update(tree_updated, State) ->
    Built = State#state.built + 1,
    case Built of
        2 ->
            {next_state, exchange, State, 0};
        _ ->
            {next_state, update, State#state{built=Built}}
    end;
update({update_error, _Error}, State) ->
    {stop, normal, State}.

exchange(timeout, State=#state{peer=Peer}) ->
    RemoteFun = fun(Prefixes, {get_bucket, {Level, Bucket}}) ->
                        riak_core_metadata_hashtree:get_bucket(Peer, Prefixes, Level, Bucket);
                   (Prefixes, {key_hashes, Segment}) ->
                        riak_core_metadata_hashtree:key_hashes(Peer, Prefixes, Segment)
                end,
    HandlerFun = fun(Diff, Acc) ->
                         repair(Peer, Diff),
                         track_repair(Diff, Acc)
                 end,
    Res = riak_core_metadata_hashtree:compare(RemoteFun, HandlerFun,
                                              #exchange{local=0,remote=0,keys=0}),
    #exchange{local=LocalPrefixes,
              remote=RemotePrefixes,
              keys=Keys} = Res,
    Total = LocalPrefixes + RemotePrefixes + Keys,
    case Total > 0 of
        true ->
            lager:info("completed metadata exchange with ~p. repaired ~p missing local prefixes, "
                       "~p missing remote prefixes, and ~p keys", [Peer, LocalPrefixes, RemotePrefixes, Keys]);
        false ->
            lager:debug("completed metadata exchange with ~p. nothing repaired", [Peer])
    end,
    {stop, normal, State}.

prepare(_Event, _From, State) ->
    {reply, ok, prepare, State}.

update(_Event, _From, State) ->
    {reply, ok, update, State}.

exchange(_Event, _From, State) ->
    {reply, ok, exchange, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
repair(Peer, {missing_prefix, Type, Prefix}) ->
    repair_prefix(Peer, Type, Prefix);
repair(Peer, {key_diffs, Prefix, Diffs}) ->
    _ = [repair_keys(Peer, Prefix, Diff) || Diff <- Diffs],
    ok.

%% @private
repair_prefix(Peer, Type, [Prefix]) ->
    ItType = repair_iterator_type(Type),
    repair_sub_prefixes(Type, Peer, Prefix, repair_iterator(ItType, Peer, Prefix));
repair_prefix(Peer, Type, [Prefix, SubPrefix]) ->
    FullPrefix = {Prefix, SubPrefix},
    ItType = repair_iterator_type(Type),
    repair_full_prefix(Type, Peer, FullPrefix, repair_iterator(ItType, Peer, FullPrefix)).

%% @private
repair_sub_prefixes(Type, Peer, Prefix, It) ->
    case riak_core_metadata_manager:iterator_done(It) of
        true ->
            riak_core_metadata_manager:iterator_close(It);
        false ->
            SubPrefix = riak_core_metadata_manager:iterator_value(It),
            FullPrefix = {Prefix, SubPrefix},

            ItType = repair_iterator_type(Type),
            ObjIt = repair_iterator(ItType, Peer, FullPrefix),
            repair_full_prefix(Type, Peer, FullPrefix, ObjIt),
            repair_sub_prefixes(Type, Peer, Prefix,
                                riak_core_metadata_manager:iterate(It))
    end.

%% @private
repair_full_prefix(Type, Peer, FullPrefix, ObjIt) ->
    case riak_core_metadata_manager:iterator_done(ObjIt) of
        true ->
            riak_core_metadata_manager:iterator_close(ObjIt);
        false ->
            {Key, Obj} = riak_core_metadata_manager:iterator_value(ObjIt),
            repair_other(Type, Peer, {FullPrefix, Key}, Obj),
            repair_full_prefix(Type, Peer, FullPrefix,
                               riak_core_metadata_manager:iterate(ObjIt))
    end.

%% @private
repair_other(local, _Peer, PKey, Obj) ->
    %% local missing data, merge remote data locally
    merge(undefined, PKey, Obj);
repair_other(remote, Peer, PKey, Obj) ->
    %% remote missing data, merge local data into remote node
    merge(Peer, PKey, Obj).

%% @private
repair_keys(Peer, PrefixList, {_Type, KeyBin}) ->
    Key = binary_to_term(KeyBin),
    Prefix = list_to_tuple(PrefixList),
    PKey = {Prefix, Key},
    LocalObj = riak_core_metadata_manager:get(PKey),
    RemoteObj = riak_core_metadata_manager:get(Peer, PKey),
    merge(undefined, PKey, RemoteObj),
    merge(Peer, PKey, LocalObj),
    ok.

%% @private
%% context is ignored since its in object, so pass undefined
merge(undefined, PKey, RemoteObj) ->
    riak_core_metadata_manager:merge({PKey, undefined}, RemoteObj);
merge(Peer, PKey, LocalObj) ->
    riak_core_metadata_manager:merge(Peer, {PKey, undefined}, LocalObj).


%% @private
repair_iterator(local, _, Prefix) when is_atom(Prefix) orelse is_binary(Prefix) ->
    riak_core_metadata_manager:iterator(Prefix);
repair_iterator(local, _, Prefix) when is_tuple(Prefix) ->
    riak_core_metadata_manager:iterator(Prefix, undefined);
repair_iterator(remote, Peer, PrefixOrFull) ->
    riak_core_metadata_manager:remote_iterator(Peer, PrefixOrFull).

%% @private
repair_iterator_type(local) ->
    %% local node missing prefix, need to iterate remote
    remote;
repair_iterator_type(remote) ->
    %% remote node missing prefix, need to iterate local
    local.

%% @private
track_repair({missing_prefix, local, _}, Acc=#exchange{local=Local}) ->
    Acc#exchange{local=Local+1};
track_repair({missing_prefix, remote, _}, Acc=#exchange{remote=Remote}) ->
    Acc#exchange{remote=Remote+1};
track_repair({key_diffs, _, Diffs}, Acc=#exchange{keys=Keys}) ->
    Acc#exchange{keys=Keys+length(Diffs)}.
%% @private
remote_lock_request(Peer) ->
    Self = self(),
    as_event(fun() ->
                     Res = riak_core_metadata_hashtree:lock(Peer, Self),
                     {remote_lock, Res}
             end).

%% @private
update_request(Node) ->
    as_event(fun() ->
                     %% acquired lock so we know there is no other update
                     %% and tree is built
                     case riak_core_metadata_hashtree:update(Node) of
                         ok -> tree_updated;
                         Error -> {update_error, Error}
                     end
             end).

%% @private
%% "borrowed" from riak_kv_exchange_fsm
as_event(F) ->
    Self = self(),
    spawn_link(fun() ->
                       Result = F(),
                       gen_fsm:send_event(Self, Result)
               end),
    ok.
