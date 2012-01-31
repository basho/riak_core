%% -------------------------------------------------------------------
%%
%% Riak: A lightweight, decentralized key-value store.
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_core).
-export([stop/0, stop/1, join/1, join/4, remove/1, down/1, leave/0,
         remove_from_cluster/1]).
-export([vnode_modules/0]).
-export([register/1, register/2, bucket_fixups/0, bucket_validators/0]).

-export([add_guarded_event_handler/3, add_guarded_event_handler/4]).
-export([delete_guarded_event_handler/3]).
-export([wait_for_application/1, wait_for_service/1]).
-compile({no_auto_import,[register/2]}).

-define(WAIT_PRINT_INTERVAL, (60 * 1000)).
-define(WAIT_POLL_INTERVAL, 100).

%% @spec stop() -> ok
%% @doc Stop the riak application and the calling process.
stop() -> stop("riak stop requested").

-ifdef(TEST).
stop(Reason) ->
    lager:notice("~p", [Reason]),
    % if we're in test mode, we don't want to halt the node, so instead
    % we just stop the application.
    application:stop(riak_core).
-else.
stop(Reason) ->
    % we never do an application:stop because that makes it very hard
    %  to really halt the runtime, which is what we need here.
    lager:notice("~p", [Reason]),
    init:stop().
-endif.

%%
%% @doc Join the ring found on the specified remote node
%%
join(NodeStr) when is_list(NodeStr) ->
    join(riak_core_util:str_to_node(NodeStr));
join(Node) when is_atom(Node) ->
    join(node(), Node).

join(Node, Node) ->
    {error, self_join};
join(_, Node) ->
    join(riak_core_gossip:legacy_gossip(), node(), Node, false).

join(true, _, Node, _Rejoin) ->
    legacy_join(Node);
join(false, _, Node, Rejoin) ->
    case net_adm:ping(Node) of
        pang ->
            {error, not_reachable};
        pong ->
            case rpc:call(Node, riak_core_gossip, legacy_gossip, []) of
                true ->
                    legacy_join(Node);
                _ ->
                    %% Failure due to trying to join older node that
                    %% doesn't define legacy_gossip will be handled
                    %% in standard_join based on seeing a legacy ring.
                    standard_join(Node, Rejoin)
            end
    end.

get_other_ring(Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_my_ring, []) of
        {ok, Ring} ->
            case riak_core_ring:legacy_ring(Ring) of
                true ->
                    {ok, Ring};
                false ->
                    rpc:call(Node, riak_core_ring_manager, get_raw_ring, [])
            end;
        Error ->
            Error
    end.

standard_join(Node, Rejoin) when is_atom(Node) ->
    case net_adm:ping(Node) of
        pong ->
            case get_other_ring(Node) of
                {ok, Ring} ->
                    case riak_core_ring:legacy_ring(Ring) of
                        true ->
                            legacy_join(Node);
                        false ->
                            standard_join(Node, Ring, Rejoin)
                    end;
                _ -> 
                    {error, unable_to_get_join_ring}
            end;
        pang ->
            {error, not_reachable}
    end.

standard_join(Node, Ring, Rejoin) ->
    {ok, MyRing} = riak_core_ring_manager:get_raw_ring(),
    SameSize = (riak_core_ring:num_partitions(MyRing) =:=
                riak_core_ring:num_partitions(Ring)),
    Singleton = ([node()] =:= riak_core_ring:all_members(MyRing)),
    case {Rejoin or Singleton, SameSize} of
        {false, _} ->
            {error, not_single_node};
        {_, false} ->
            {error, different_ring_sizes};
        _ ->
            GossipVsn = riak_core_gossip:gossip_version(),
            Ring2 = riak_core_ring:add_member(node(), Ring,
                                              node()),
            Ring3 = riak_core_ring:set_owner(Ring2, node()),
            Ring4 =
                riak_core_ring:update_member_meta(node(),
                                                  Ring3,
                                                  node(),
                                                  gossip_vsn,
                                                  GossipVsn),
            riak_core_ring_manager:set_my_ring(Ring4),
            riak_core_gossip:send_ring(Node, node())
    end.

legacy_join(Node) when is_atom(Node) ->
    {ok, OurRingSize} = application:get_env(riak_core, ring_creation_size),
    case net_adm:ping(Node) of
        pong ->
            case rpc:call(Node,
                          application,
                          get_env, 
                          [riak_core, ring_creation_size]) of
                {ok, OurRingSize} ->
                    riak_core_gossip:send_ring(Node, node());
                _ -> 
                    {error, different_ring_sizes}
            end;
        pang ->
            {error, not_reachable}
    end.

remove(Node) ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    case {riak_core_ring:all_members(Ring),
          riak_core_ring:member_status(Ring, Node)} of
        {_, invalid} ->
            {error, not_member};
        {[Node], _} ->
            {error, only_member};
        _ ->
            case riak_core_gossip:legacy_gossip() of
                true ->
                    legacy_remove(Node);
                false ->
                    standard_remove(Node)
            end
    end.

standard_remove(Node) ->
    riak_core_ring_manager:ring_trans(
      fun(Ring2, _) -> 
              Ring3 = riak_core_ring:remove_member(node(), Ring2, Node),
              Ring4 = riak_core_ring:ring_changed(node(), Ring3),
              {new_ring, Ring4}
      end, []),
    ok.

down(Node) ->
    down(riak_core_gossip:legacy_gossip(), Node).
down(true, _) ->
    {error, legacy_mode};
down(false, Node) ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    case net_adm:ping(Node) of
        pong ->
            {error, is_up};
        pang ->
            case {riak_core_ring:all_members(Ring),
                  riak_core_ring:member_status(Ring, Node)} of
                {_, invalid} ->
                    {error, not_member};
                {[Node], _} ->
                    {error, only_member};
                _ ->
                    riak_core_ring_manager:ring_trans(
                      fun(Ring2, _) -> 
                              Ring3 = riak_core_ring:down_member(node(), Ring2, Node),
                              Ring4 = riak_core_ring:ring_changed(node(), Ring3),
                              {new_ring, Ring4}
                      end, []),
                    ok
            end
    end.

leave() ->
    Node = node(),
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    case {riak_core_ring:all_members(Ring),
          riak_core_ring:member_status(Ring, Node)} of
        {_, invalid} ->
            {error, not_member};
        {[Node], _} ->
            {error, only_member};
        {_, valid} ->
            case riak_core_gossip:legacy_gossip() of
                true ->
                    legacy_remove(Node);
                false ->
                    standard_leave(Node)
            end;
        {_, _} ->
            {error, already_leaving}
    end.

standard_leave(Node) ->
    riak_core_ring_manager:ring_trans(
      fun(Ring2, _) -> 
              Ring3 = riak_core_ring:leave_member(Node, Ring2, Node),
              {new_ring, Ring3}
      end, []),
    ok.

%% @spec remove_from_cluster(ExitingNode :: atom()) -> term()
%% @doc Cause all partitions owned by ExitingNode to be taken over
%%      by other nodes.
remove_from_cluster(ExitingNode) when is_atom(ExitingNode) ->
    remove(ExitingNode).

legacy_remove(Node) when is_atom(Node) ->
    case catch(riak_core_gossip_legacy:remove_from_cluster(Node)) of
        {'EXIT', {badarg, [{erlang, hd, [[]]}|_]}} ->
            %% This is a workaround because
            %% riak_core_gossip:remove_from_cluster doesn't check if
            %% the result of subtracting the current node from the
            %% cluster member list results in the empty list. When
            %% that code gets refactored this can probably go away.
            {error, only_member};
        ok ->
            ok
    end.

vnode_modules() ->
    case application:get_env(riak_core, vnode_modules) of
        undefined -> [];
        {ok, Mods} -> Mods
    end.

bucket_fixups() ->
     case application:get_env(riak_core, bucket_fixups) of
        undefined -> [];
        {ok, Mods} -> Mods
    end.

bucket_validators() ->
    case application:get_env(riak_core, bucket_validators) of
        undefined -> [];
        {ok, Mods} -> Mods
    end.

%% Get the application name if not supplied, first by get_application
%% then by searching by module name
get_app(undefined, Module) ->
    {ok, App} = case application:get_application(self()) of
                    {ok, AppName} -> {ok, AppName};
                    undefined -> app_for_module(Module)
                end,
    App;
get_app(App, _Module) ->
    App.

%% @doc Register a riak_core application.
register(Props) ->
    register(undefined, Props).

%% @doc Register a named riak_core application.
register(_App, []) ->
    %% Once the app is registered, do a no-op ring trans
    %% to ensure the new fixups are run against
    %% the ring.
    {ok, _R} = riak_core_ring_manager:ring_trans(fun(R,_A) -> {new_ring, R} end,
                                                 undefined),
    riak_core_ring_events:force_sync_update(),
    ok;
register(App, [{bucket_fixup, FixupMod}|T]) ->
    register_mod(get_app(App, FixupMod), FixupMod, bucket_fixups),
    register(App, T);
register(App, [{repl_helper, FixupMod}|T]) ->
    register_mod(get_app(App, FixupMod), FixupMod, repl_helper),
    register(App, T);
register(App, [{vnode_module, VNodeMod}|T]) ->
    register_mod(get_app(App, VNodeMod), VNodeMod, vnode_modules),
    register(App, T);
register(App, [{bucket_validator, ValidationMod}|T]) ->
    register_mod(get_app(App, ValidationMod), ValidationMod, bucket_validators),
    register(App, T).

register_mod(App, Module, Type) when is_atom(Module), is_atom(Type) ->
    case Type of
        vnode_modules ->
            riak_core_vnode_proxy_sup:start_proxies(Module);
        _ ->
            ok
    end,
    case application:get_env(riak_core, Type) of
        undefined ->
            application:set_env(riak_core, Type, [{App,Module}]);
        {ok, Mods} ->
            application:set_env(riak_core, Type,
                lists:usort([{App,Module}|Mods]))
    end.

%% @spec add_guarded_event_handler(HandlerMod, Handler, Args) -> AddResult
%%       HandlerMod = module()
%%       Handler = module() | {module(), term()}
%%       Args = list()
%%       AddResult = ok | {error, Reason::term()}
add_guarded_event_handler(HandlerMod, Handler, Args) ->
    add_guarded_event_handler(HandlerMod, Handler, Args, undefined).

%% @spec add_guarded_event_handler(HandlerMod, Handler, Args, ExitFun) -> AddResult
%%       HandlerMod = module()
%%       Handler = module() | {module(), term()}
%%       Args = list()
%%       ExitFun = fun(Handler, Reason::term())
%%       AddResult = ok | {error, Reason::term()}
%%
%% @doc Add a "guarded" event handler to a gen_event instance.  
%%      A guarded handler is implemented as a supervised gen_server 
%%      (riak_core_eventhandler_guard) that adds a supervised handler in its 
%%      init() callback and exits when the handler crashes so it can be
%%      restarted by the supervisor.
add_guarded_event_handler(HandlerMod, Handler, Args, ExitFun) ->
    riak_core_eventhandler_sup:start_guarded_handler(HandlerMod, Handler, Args, ExitFun).

%% @spec delete_guarded_event_handler(HandlerMod, Handler, Args) -> Result
%%       HandlerMod = module()
%%       Handler = module() | {module(), term()}
%%       Args = term()
%%       Result = term() | {error, module_not_found} | {'EXIT', Reason}
%%       Reason = term()
%%
%% @doc Delete a guarded event handler from a gen_event instance.
%% 
%%      Args is an arbitrary term which is passed as one of the arguments to 
%%      Module:terminate/2.
%%
%%      The return value is the return value of Module:terminate/2. If the 
%%      specified event handler is not installed, the function returns 
%%      {error,module_not_found}. If the callback function fails with Reason, 
%%      the function returns {'EXIT',Reason}.
delete_guarded_event_handler(HandlerMod, Handler, Args) ->
    riak_core_eventhandler_sup:stop_guarded_handler(HandlerMod, Handler, Args).

app_for_module(Mod) ->
    app_for_module(application:which_applications(), Mod).

app_for_module([], _Mod) ->
    {ok, undefined};
app_for_module([{App,_,_}|T], Mod) ->
    {ok, Mods} = application:get_key(App, modules),
    case lists:member(Mod, Mods) of
        true -> {ok, App};
        false -> app_for_module(T, Mod)
    end.


wait_for_application(App) ->
    wait_for_application(App, 0).
wait_for_application(App, Elapsed) ->
    case lists:keymember(App, 1, application:which_applications()) of
        true when Elapsed == 0 ->
            ok;
        true when Elapsed > 0 ->
            lager:info("Wait complete for application ~p (~p seconds)", [App, Elapsed div 1000]),
            ok;
        false ->
            %% Possibly print a notice.
            ShouldPrint = Elapsed rem ?WAIT_PRINT_INTERVAL == 0,
            case ShouldPrint of
                true -> lager:info("Waiting for application ~p to start (~p seconds).", [App, Elapsed div 1000]);
                false -> skip
            end,
            timer:sleep(?WAIT_POLL_INTERVAL),
            wait_for_application(App, Elapsed + ?WAIT_POLL_INTERVAL)
    end.

wait_for_service(Service) ->
    wait_for_service(Service, 0).
wait_for_service(Service, Elapsed) ->
    case lists:member(Service, riak_core_node_watcher:services(node())) of
        true when Elapsed == 0 ->
            ok;
        true when Elapsed > 0 ->
            lager:info("Wait complete for service ~p (~p seconds)", [Service, Elapsed div 1000]),
            ok;
        false ->
            %% Possibly print a notice.
            ShouldPrint = Elapsed rem ?WAIT_PRINT_INTERVAL == 0,
            case ShouldPrint of
                true -> lager:info("Waiting for service ~p to start (~p seconds)", [Service, Elapsed div 1000]);
                false -> skip
            end,
            timer:sleep(?WAIT_POLL_INTERVAL),
            wait_for_service(Service, Elapsed + ?WAIT_POLL_INTERVAL)
    end.
