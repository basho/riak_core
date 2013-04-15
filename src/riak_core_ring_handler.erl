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
    maybe_start_vnode_proxies(Ring),
    maybe_stop_vnode_proxies(Ring),
    riak_core_vnode_manager:ring_changed(Ring),
    riak_core_capability:ring_changed(Ring),
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
                       ok = riak_core:wait_for_application(App),

                       %% Start the vnodes.
                       HasStartVnodes = lists:member({start_vnodes, 1},
                                                     Mod:module_info(exports)),
                       case HasStartVnodes of
                           true ->
                               Mod:start_vnodes(Startable);
                           false ->
                               [Mod:start_vnode(I) || I <- Startable]
                       end,

                       %% Mark the service as up.
                       SupName = list_to_atom(atom_to_list(App) ++ "_sup"),
                       SupPid = erlang:whereis(SupName),
                       case riak_core:health_check(App) of
                           undefined ->
                               riak_core_node_watcher:service_up(App, SupPid);
                           HealthMFA ->
                               riak_core_node_watcher:service_up(App,
                                                                 SupPid,
                                                                 HealthMFA)
                       end,
                       exit(normal)
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

maybe_start_vnode_proxies(Ring) ->
    Mods = [M || {_,M} <- riak_core:vnode_modules()],
    Size = riak_core_ring:num_partitions(Ring),
    FutureSize = riak_core_ring:future_num_partitions(Ring),
    Larger = Size < FutureSize,
    case Larger of
        true ->
            FutureIdxs = riak_core_ring:all_next_owners(Ring),
            [riak_core_vnode_proxy_sup:start_proxy(Mod, Idx) || {Idx, _} <- FutureIdxs,
                                                                Mod <- Mods],
            ok;
        false ->
            ok
    end.

maybe_stop_vnode_proxies(Ring) ->
    Mods = [M || {_, M} <- riak_core:vnode_modules()],
    case riak_core_ring:pending_changes(Ring) of
        [] ->
            Idxs = [{I,M} || {I,_} <- riak_core_ring:all_owners(Ring), M <- Mods],
            ProxySpecs = supervisor:which_children(riak_core_vnode_proxy_sup),
            Running = [{I,M} || {{M,I},Pid,_,_} <- ProxySpecs,
                            is_pid(Pid) andalso is_process_alive(Pid)],
            ToShutdown = Running -- Idxs,
            [riak_core_vnode_proxy_sup:stop_proxy(M,I) || {I, M} <- ToShutdown];
        _ ->
            ok
    end.
