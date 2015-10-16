%%% ===================================================================
%%% 
%%% riak_core_metadata_evt_sup supervises all event managers for
%%% metadata events. One event manager is created per prefix
%%% that is subscribed to. Events that are not subscribed to do
%%% nothing.
%%%
%%% Copyright (c) 2015 Basho Technologies, Inc.  All Rights Reserved.
%%%
%%% This file is provided to you under the Apache License,
%%% Version 2.0 (the "License"); you may not use this file
%%% except in compliance with the License.  You may obtain
%%% a copy of the License at
%%%
%%%   http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing,
%%% software distributed under the License is distributed on an
%%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%%% KIND, either express or implied.  See the License for the
%%% specific language governing permissions and limitations
%%% under the License.
%%%
%%% ===================================================================

-module(riak_core_metadata_evt_sup).
-behaviour(supervisor).

-export([init/1]).
-export([is_type_compiled/2]).
-export([start_link/0]).
-export([swap_notification_handler/3]).
-export([sync_notify/2]).

-include("riak_core_bucket_type.hrl").

-define(TABLE, ?MODULE).
-define(CHILD_ID, riak_core_metadata_evt).

%%% ===================================================================
%%% API
%%% ===================================================================

%%
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% Swap the existing `Handler' with the given one, or create a new one
%% if one does not exist. Prevents event handlers being added multiple
%% times.
swap_notification_handler(FullPrefix, Handler, HandlerArgs) ->
    case ets:lookup(?TABLE, FullPrefix) of
        [{FullPrefix, Pid}] ->
            swap_notification_handler2(Pid, Handler, HandlerArgs);
        [] ->
            {ok, Pid} = supervisor:start_child(?MODULE, []) ,
            true = ets:insert_new(?TABLE, {FullPrefix, Pid}),
            swap_notification_handler2(Pid, Handler, HandlerArgs)
    end.

%% Notify all listeners that metadata has been stored, this could mean
%% that a new bucket type has been created or that it has been updated.
%% Listeners receive the event type `{metadata_stored, Key::any()}'.
sync_notify(FullPrefix, Key) ->
    case (catch ets:lookup(?TABLE, FullPrefix)) of
        [{FullPrefix, Pid}] ->
            gen_event:sync_notify(Pid, {metadata_stored, Key});
        _ ->
            ok
    end.

%% Make a call to bucket type listeners if the bucket type ddl has been
%% compiled yet, if any of the listeners returns true then it has. Listeners
%% that do not handle this should return false, or any value that is not true.
%%
%% This should only be called if the bucket type has the ddl property.
-spec is_type_compiled(BucketType :: binary(), DDL :: term()) -> boolean().
is_type_compiled(BucketType, DDL) when is_binary(BucketType) ->
    case ets:lookup(?TABLE, ?BUCKET_TYPE_PREFIX) of
        [{?BUCKET_TYPE_PREFIX, Pid}] ->
            Handlers = gen_event:which_handlers(Pid),
            Req = {is_type_compiled, [BucketType, DDL]},
            Results = [gen_event:call(Pid, H, Req) || H <- Handlers],
            lists:member(true, Results);
        [] ->
            false
    end.

%%% ===================================================================
%%% Internal functions
%%% ===================================================================

%%
init([]) ->
    % an ets table is used to map prefixes to the event manager process
    ?TABLE = ets:new(?TABLE, [public, named_table]),

    Procs = [{?CHILD_ID,
             {gen_event, start_link, []},
              permanent, 1000, worker, []} ],
    {ok, {{simple_one_for_one, 1, 5}, Procs}}.

%%
swap_notification_handler2(Pid, Handler, HandlerArgs) when is_pid(Pid) ->
    Terminate_args = [],
    gen_event:swap_handler(
        Pid, {Handler, Terminate_args}, {Handler, HandlerArgs}).


%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(in_process(TestCode),
    Self = self(),
    spawn_link(
        fun() ->
            TestCode,
            Self ! test_ok
        end),
    receive
        test_ok -> ok
    end
).

start_link_test() ->
    ?in_process(
        begin
            ?assertMatch(
                {ok, _},
                ?MODULE:start_link()
            )
        end).


swap_notification_handler_test() ->
    ?in_process(
        begin
            Metadata_type = {core,bucket_types},
            {ok, _} = riak_core_metadata_evt_sup:start_link(),
            ok = riak_core_metadata_evt_sup:swap_notification_handler(
                Metadata_type, dummy_evt, []),
            ?assertMatch(
                [{Metadata_type, _}],
                ets:tab2list(?TABLE)
            )
        end).

-endif.
