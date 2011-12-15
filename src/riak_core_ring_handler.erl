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
    riak_core_vnode_manager:ring_changed(Ring),
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
                [] ->
                    Legacy = riak_core_gossip:legacy_gossip(),
                    Ready = riak_core_ring:ring_ready(Ring),
                    FutureIndices = riak_core_ring:future_indices(Ring, node()),
                    Status = riak_core_ring:member_status(Ring, node()),
                    case {Legacy, Ready, FutureIndices, Status} of
                        {true, _, _, _} ->
                            riak_core_ring_manager:refresh_my_ring();
                        {_, true, [], leaving} ->
                            riak_core_ring_manager:ring_trans(
                              fun(Ring2, _) -> 
                                      Ring3 = riak_core_ring:exit_member(node(), Ring2, node()),
                                      {new_ring, Ring3}
                              end, []),
                            %% Shutdown if we are the only node in the cluster
                            case riak_core_ring:random_other_node(Ring) of
                                no_node ->
                                    riak_core_ring_manager:refresh_my_ring();
                                _ ->
                                    ok
                            end;
                        {_, _, _, invalid} ->
                            riak_core_ring_manager:refresh_my_ring();
                        {_, _, _, exiting} ->
                            %% Deliberately do nothing.
                            ok;
                        {_, _, _, _} ->
                            ok
                    end;
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
                       try erlang:register(RegName, self())
                       catch error:badarg ->
                               exit(normal)
                       end,

                       %% Let the app finish starting...
                       case riak_core:wait_for_application(App) of
                           ok ->
                               %% Start the vnodes.
                               [Mod:start_vnode(I) || I <- Startable],

                               %% Mark the service as up.
                               SupName = list_to_atom(atom_to_list(App) ++ "_sup"),
                               SupPid = erlang:whereis(SupName),
                               riak_core_node_watcher:service_up(App, SupPid),
                               exit(normal);
                           {error, Reason} ->
                               lager:critical("Failed to start application: ~p", [App]),
                               throw({error, Reason})
                       end
               end),
    Startable.


startable_vnodes(Mod, Ring) ->
    AllMembers = riak_core_ring:all_members(Ring),
    case {length(AllMembers), hd(AllMembers) =:= node()} of
        {1, true} ->
            riak_core_ring:my_indices(Ring);
        _ ->
            {ok, ModExcl} = riak_core_handoff_manager:get_exclusions(Mod),
            Excl = ModExcl -- riak_core_ring:disowning_indices(Ring, node()),
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
