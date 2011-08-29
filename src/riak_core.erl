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
-export([stop/0, stop/1, join/1, remove_from_cluster/1]).
-export([register_vnode_module/1, vnode_modules/0]).
-export([register/1, bucket_fixups/0]).
-export([add_guarded_event_handler/3, add_guarded_event_handler/4]).
-export([delete_guarded_event_handler/3]).

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

%% @spec remove_from_cluster(ExitingNode :: atom()) -> term()
%% @doc Cause all partitions owned by ExitingNode to be taken over
%%      by other nodes.
remove_from_cluster(ExitingNode) when is_atom(ExitingNode) ->
    riak_core_gossip:remove_from_cluster(ExitingNode).

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

%% @doc Register a riak_core application.
register([]) ->
    ok;
register([{bucket_fixup, FixupMod}|T]) ->
    register_riak_core_application_module(FixupMod, bucket_fixups),
    register(T);
register([{vnode_module, VNodeMod}|T]) ->
    register_riak_core_application_module(VNodeMod, vnode_modules),
    register(T).

register_vnode_module(VNodeMod) when is_atom(VNodeMod)  ->
    register_riak_core_application_module(VNodeMod, vnode_modules).

register_riak_core_application_module(Module, Key) when is_atom(Module), is_atom(Key) ->
    {ok, App} = case application:get_application(self()) of
        {ok, AppName} -> {ok, AppName};
        undefined -> app_for_module(Module)
    end,
    case application:get_env(riak_core, Key) of
        undefined ->
            application:set_env(riak_core, Key, [{App,Module}]);
        {ok, Mods} ->
            application:set_env(riak_core, Key, [{App,Module}|Mods])
    end,
    riak_core_ring_events:force_sync_update().

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
    undefined;
app_for_module([{App,_,_}|T], Mod) ->
    {ok, Mods} = application:get_key(App, modules),
    case lists:member(Mod, Mods) of
        true -> {ok, App};
        false -> app_for_module(T, Mod)
    end.
