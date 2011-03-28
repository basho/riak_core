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

%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.

-module(riak_core_ring_handler).
-behaviour(gen_event).

%% gen_event callbacks
-export([init/1, handle_event/2, handle_call/2,
         handle_info/2, terminate/2, code_change/3]).
-record(state, {}).


%% ===================================================================
%% gen_event callbacks
%% ===================================================================

init([]) ->
    %% Pull the initial ring and make sure all vnodes are started
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    ensure_vnodes_started(Ring),
    {ok, #state{}}.

handle_event({ring_update, Ring}, State) ->
    %% Make sure all vnodes are started...
    ensure_vnodes_started(Ring),
    {ok, State}.

handle_call(_Event, State) ->
    {ok, ok, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%% ===================================================================
%% Internal functions
%% ===================================================================

ensure_vnodes_started(Ring) ->
    case riak_core:vnode_modules() of
        [] ->
            ok;
        AppMods ->            
            case ensure_vnodes_started(AppMods, Ring, []) of
                [] -> riak_core_ring_manager:refresh_my_ring();
                _ -> ok
            end
    end.

ensure_vnodes_started([], _Ring, Acc) ->
    lists:flatten(Acc);
ensure_vnodes_started([{App, Mod}|T], Ring, Acc) ->
    ensure_vnodes_started(T, Ring, [ensure_vnodes_started({App,Mod},Ring)|Acc]).

ensure_vnodes_started({App,Mod}, Ring) ->
    Startable = startable_vnodes(Mod, Ring),
    %% NOTE: This following is a hack.  There's a basic
    %%       dependency/race between riak_core (want to start vnodes
    %%       right away to trigger possible handoffs) and riak_kv
    %%       (needed to support those vnodes).  The hack does not fix
    %%       that dependency: internal techdebt todo list #A7 does.
    spawn_link(fun() ->
    %%                 Use a registered name as a lock to prevent the same
    %%                 vnode module from being started twice.
                       RegName = list_to_atom(
                                   "riak_core_ring_handler_ensure_"
                                   ++ atom_to_list(Mod)),
                       try register(RegName, self())
                       catch error:badarg ->
                               exit(normal)
                       end,
                       wait_for_app(App, 100, 100),
                       [Mod:start_vnode(I) || I <- Startable],
                       exit(normal)
               end),
    Startable.

startable_vnodes(Mod, Ring) ->
    AllMembers = riak_core_ring:all_members(Ring),
    case {length(AllMembers), hd(AllMembers) =:= node()} of
        {1, true} ->
            riak_core_ring:my_indices(Ring);
        _ ->
            {ok, Excl} = riak_core_handoff_manager:get_exclusions(Mod),
            case riak_core_ring:random_other_index(Ring, Excl) of
                no_indices ->
                    case length(Excl) =:= riak_core_ring:num_partitions(Ring) of
                        true ->
                            [];
                        false ->
                            riak_core_ring:my_indices(Ring)
                    end;
                RO ->
                    [RO | riak_core_ring:my_indices(Ring)]
            end
    end.

wait_for_app(_, 0, _) ->
    bummer;
wait_for_app(App, Count, Sleep) ->
    case lists:keymember(App, 1, application:which_applications()) of
        true ->
            ok;
        false ->
            timer:sleep(Sleep),
            wait_for_app(App, Count - 1, Sleep)
    end.
