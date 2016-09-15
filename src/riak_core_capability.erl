%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc
%% This module implements a cluster capability system that tracks the modes
%% supported by different nodes in the cluster and automatically determines
%% the most preferred mode for each capability that is supported by all nodes.
%% The primary use of this system is to support seamless transitions between
%% different node versions during a rolling upgrade -- such as speaking an
%% old protocol while the cluster still contains older nodes, and then
%% switching to a newer protocol after all nodes have been upgraded.
%%
%% The capability system exposes a simple `register' and `get' API, that
%% allows applications to register a given capability and set of supported
%% modes, and then retrieve the current mode that has been safely negotiated
%% across the cluster. The system also allows overriding negotiation through
%% application environment variables (eg. in app.config).
%%
%% To register a capability and set of supported modes:
%%   Use {@link register/3} or {@link register/4}
%%
%% To query the current negotiated capability:
%%   Use {@link get/1} or {@link get/2}
%%
%% The capability system implements implicit mode preference. When registering
%% modes, the modes listed earlier in the list are preferred over modes listed
%% later in the list.
%%
%% Users can override capabilities by setting the `override_capability' app
%% variable for the appropriate application. For example, to override the
%% `{riak_core, vnode_routing}' capability, the user could add the following
%% to `riak_core' section of `app.config':
%%
%% {override_capability,
%%   [{vnode_routing,
%%     [{use, some_mode},
%%      {prefer, some_other_mode}]
%%   }]
%% }
%%
%% The two override parameters are `use' and `prefer'. The `use' parameter
%% specifies a mode that will always be used for the given capability,
%% ignoring negotiation. It is a forced override. The `prefer' parameter
%% specifies a mode that will be used if safe across the entire cluster.
%% This overrides the built-in mode preference, but still only selects the
%% mode if safe. When both `use' and `prefer' are specified, `use' takes
%% precedence.
%%
%% There is no inherent upgrading/downgrading of protocols in this system.
%% The system is designed with the assumption that all supported modes can
%% be used at any time (even concurrently), and is concerned solely with
%% selecting the most preferred mode common across the cluster at a given
%% point in time.

-module(riak_core_capability).
-behaviour(gen_server).

%% API
-export([start_link/0,
         register/4,
         register/3,
         get/1,
         get/2,
         all/0,
         update_ring/1]).

-export([make_capability/4,
         preferred_modes/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-type capability() :: atom() | {atom(), atom()}.
-type mode() :: term().

-record(capability, {supported :: [mode()],
                     default :: mode(),
                     legacy}).

-type registered() :: [{capability(), #capability{}}].

-record(state, {registered :: registered(),
                last_ring_id :: term(),
                supported :: [{node(), [{capability(), [mode()]}]}],
                unknown :: [node()],
                negotiated :: [{capability(), mode()}]
               }).

-define(ETS, riak_capability_ets).
-define(CAPS, '$riak_capabilities').

-ifdef(TEST).
-compile(export_all).
-type state() :: #state{}.
-export_type([state/0]).

-endif.

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Register a new capability providing a list of supported modes, the
%% default mode, and an optional mapping of how a legacy application variable
%% maps to different modes. The order of modes in `Supported' determines the
%% mode preference -- modes listed earlier are more preferred.
register(Capability, Supported, Default, LegacyVar) ->
    Info = capability_info(Supported, Default, LegacyVar),
    gen_server:call(?MODULE, {register, Capability, Info}, infinity),
    ok.

%% @doc Register a new capability providing a list of supported modes as well
%% as the default value. The order of modes in `Supported' determines the mode
%% preference -- modes listed earlier are more preferred.
register(Capability, Supported, Default) ->
    register(Capability, Supported, Default, undefined).

%% @doc Query the current negotiated mode for a given capability, throwing an
%%      exception if the capability is unknown or the capability system is
%%      unavailable.
get(Capability) ->
    case get(Capability, '$unknown') of
        '$unknown' ->
            throw({unknown_capability, Capability});
        Result ->
            Result
    end.

%% @doc Query the current negotiated mode for a given capability, returning
%%      `Default' if the capability system is unavailable.
get(Capability, Default) ->
    try
        case ets:lookup(?ETS, Capability) of
            [] ->
                Default;
            [{Capability, Choice}] ->
                Choice
        end
    catch
        _:_ ->
            Default
    end.

-ifdef(TEST).
%% @doc Exported for testing - takes opaque state record and returns negotiated
get_negotiated(State) ->
    State#state.negotiated.
-endif.

%% @doc Return a list of all negotiated capabilities
all() ->
    ets:tab2list(?ETS).

%% @doc Add the local node's supported capabilities to the given
%% ring. Currently used during the `riak-admin join' process
update_ring(Ring) ->
    %% If a join occurs immediately after a node has started, it is
    %% possible that the ETS table does not yet exist, or that the
    %% '$supported' key has not yet been written. Therefore, we catch
    %% any errors and return an unmodified ring.
    Supported = try
                    [{_, Sup}] = ets:lookup(?ETS, '$supported'),
                    Sup
                catch
                    _:_ ->
                        error
                end,
    case Supported of
        error ->
            {false, Ring};
        _ ->
            add_supported_to_ring(node(), Supported, Ring)
    end.

%% @doc
%% Make a capbility from a capability atom, a list of supported modes,
%% the default mode, and a mapping from a legacy var to it's capabilities.
-spec make_capability(capability(), [mode()], mode(), term())
                     -> {capability(), #capability{}}.
make_capability(Capability, Supported, Default, Legacy) ->
    {Capability, capability_info(Supported, Default, Legacy)}.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    ?ETS = ets:new(?ETS, [named_table, {read_concurrency, true}]),
    schedule_tick(),
    Registered = load_registered(),
    State = init_state(Registered),
    State2 = reload(State),
    {ok, State2}.

init_state(Registered) ->
    #state{registered=Registered,
           supported=[],
           unknown=[],
           negotiated=[]}.

handle_call({register, Capability, Info}, _From, State) ->
    State2 = register_capability(node(), Capability, Info, State),
    State3 = update_supported(State2),
    State4 = renegotiate_capabilities(State3),
    publish_supported(State4),
    update_local_cache(State4),
    save_registered(State4#state.registered),
    {reply, ok, State4}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(tick, State) ->
    schedule_tick(),
    State2 = maybe_update_supported(State),
    State3 =
        lists:foldl(fun(Node, StateAcc) ->
                            add_node(Node, [], StateAcc)
                    end, State2, State2#state.unknown),
    State4 = renegotiate_capabilities(State3),
    {noreply, State4};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

maybe_update_supported(State=#state{last_ring_id=LastID}) ->
    case riak_core_ring_manager:get_ring_id() of
        LastID ->
            State;
        RingID ->
            {ok, Ring} = riak_core_ring_manager:get_my_ring(),
            State2 = update_supported(Ring, State),
            State2#state{last_ring_id=RingID}
    end.

capability_info(Supported, Default, Legacy) ->
    #capability{supported=Supported, default=Default, legacy=Legacy}.

schedule_tick() ->
    Tick = app_helper:get_env(riak_core,
                              capability_tick,
                              10000),
    erlang:send_after(Tick, ?MODULE, tick).

%% Capabilities are re-initialized if riak_core_capability server crashes
reload(State=#state{registered=[]}) ->
    State;
reload(State) ->
    lager:info("Reloading capabilities"),
    State2 =
        orddict:fold(
          fun(Capability, Info, S) ->
                  S2 = add_registered(Capability, Info, S),
                  S3 = add_supported(node(), Capability,
                                     Info#capability.supported, S2),
                  S3
          end, State, State#state.registered),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    State3 = update_supported(Ring, State2),
    update_local_cache(State3),
    save_registered(State3#state.registered),
    State3.

update_supported(State) ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    update_supported(Ring, State).

%% Update this node's view of cluster capabilities based on a received ring
update_supported(Ring, State) ->
    AllSupported = get_supported_from_ring(Ring),
    State2 = remove_members(Ring, State),
    State3 =
    lists:foldl(fun({Node, _}, StateAcc) when Node == node() ->
                        StateAcc;
                   ({Node, Supported}, StateAcc) ->
                        Known = get_supported(Node, StateAcc),
                        case {Supported, Known} of
                            {[], []} ->
                                add_node(Node, Supported, StateAcc);
                            {[], _} ->
                                add_node(Node, Supported, StateAcc);
                            {Same, Same} ->
                                StateAcc;
                            {_, _} ->
                                add_node(Node, Supported, StateAcc)
                        end
                end, State2, AllSupported),
    State4 = renegotiate_capabilities(State3),
    State4.

register_capability(Node, Capability, Info, State) ->
    State2 = add_registered(Capability, Info, State),
    State3 =
        add_supported(Node, Capability, Info#capability.supported, State2),
    State3.

add_registered(Capability, Info, State) ->
    Registered = orddict:store(Capability, Info, State#state.registered),
    State#state{registered=Registered}.

get_supported(Node, #state{supported=Supported}) ->
    case orddict:find(Node, Supported) of
        {ok, Caps} ->
            Caps;
        error ->
            orddict:new()
    end.

add_supported(Node, Capability, Supported, State) ->
    Sup = get_supported(Node, State),
    Sup2 = orddict:store(Capability, Supported, Sup),
    NodeSupported = orddict:store(Node, Sup2, State#state.supported),
    State2 = State#state{supported=NodeSupported},
    State2.

%% Clear any capability information associated with nodes that are no longer
%% members of the cluster
remove_members(Ring, State=#state{supported=Supported}) ->
    Members = riak_core_ring:all_members(Ring),
    Supported2 =
        orddict:filter(fun(Node, _) ->
                               lists:member(Node, Members)
                       end, Supported),
    State2 = State#state{supported=Supported2},
    State2.

%% Add another member to the local view of cluster capabilities.  If the node
%% has published capability information in the ring, use it. Otherwise, try
%% to determine capabilities through RPC to the node. If RPC fails, use
%% default values. However, unresolved nodes will be marked as such and RPC
%% re-attempted at the next server tick.
add_node(Node, [], State=#state{unknown=Unknown}) ->
    {Capabilities, Resolved} = query_capabilities(Node, State),
    Unknown2 = case Resolved of
                   true ->
                       ordsets:del_element(Node, Unknown);
                   false ->
                       ordsets:add_element(Node, Unknown)
               end,
    State2 = State#state{unknown=Unknown2},
    add_node_capabilities(Node, Capabilities, State2);
add_node(Node, Capabilities, State) ->
    add_node_capabilities(Node, Capabilities, State).

add_node_capabilities(Node, Capabilities, State) ->
    lists:foldl(fun({Capability, Supported}, StateAcc) ->
                        add_supported(Node, Capability, Supported, StateAcc)
                end, State, Capabilities).

%% We maintain a cached-copy of the local node's supported capabilities
%% in our existing capability ETS table. This allows update_ring/1
%% to update rings without going through the capability server.
update_local_cache(State) ->
    Supported = get_supported(node(), State),
    ets:insert(?ETS, {'$supported', Supported}),
    ok.

%% Publish the local node's supported modes in the ring
publish_supported(State) ->
    Node = node(),
    Supported = get_supported(Node, State),
    F = fun(Ring, _) ->
                {Changed, Ring2} =
                    add_supported_to_ring(Node, Supported, Ring),
                case Changed of
                    true ->
                        {new_ring, Ring2};
                    false ->
                        ignore
                end
        end,
    riak_core_ring_manager:ring_trans(F, ok),
    ok.

%% Add a node's capabilities to the provided ring
add_supported_to_ring(Node, Supported, Ring) ->
    Current = riak_core_ring:get_member_meta(Ring, Node, ?CAPS),
    case Current of
        Supported ->
            {false, Ring};
        _ ->
            Ring2 = riak_core_ring:update_member_meta(Node, Ring, Node,
                                                      ?CAPS, Supported),
            {true, Ring2}
    end.

%% @doc
%% Given my node's capabilities, my node's registered default modes, the
%% list of application env overrides, and the current view of all node's
%% supported capabilities, determine the most preferred mode for each capability
%% that is supported by all nodes.
-spec preferred_modes([{capability(), [mode()]}],
                      [{node(), [{capability(), [mode()]}]}],
                       registered(),
                       [{capability(), [mode()]}])
                      -> [{capability(), mode()}].
preferred_modes(MyCaps, Capabilities, Registered, Override) ->
    N1 = reformat_capabilities(Registered, Capabilities),
    N2 = intersect_capabilities(N1),
    N3 = order_by_preference(MyCaps, N2),
    N4 = override_capabilities(N3, Override),
    N5 = [{Cap, hd(Common)} || {Cap, Common} <- N4],
    N5.

%% Given the current view of each node's supported capabilities, determine
%% the most preferred mode for each capability that is supported by all nodes
%% in the cluster.
negotiate_capabilities(Node, Override, State=#state{registered=Registered,
                                                    supported=Capabilities}) ->
    case orddict:find(Node, Capabilities) of
        error ->
            State;
        {ok, MyCaps} ->
            N = preferred_modes(MyCaps, Capabilities, Registered, Override),
            State#state{negotiated=N}
    end.

renegotiate_capabilities(State=#state{supported=[]}) ->
    State;
renegotiate_capabilities(State) ->
    Caps = orddict:fetch(node(), State#state.supported),
    Overrides = get_overrides(Caps),
    State2 = negotiate_capabilities(node(), Overrides, State),
    process_capability_changes(State#state.negotiated,
                               State2#state.negotiated),
    State2.

%% Known capabilities are tracked based on node:
%%
%% [{Node1, [{capability1, [x,y,z]},
%%           {capability2, [x,y,z]}]},
%%  {Node2, [{capability1, [a,b,z]}]}].
%%
%% Here we convert this data into a capability-centric structure:
%%
%% [{capability1, [{Node1, [x,y,z,default]}, {Node2, [a,b,c,default]}]},
%%  {capability2, [{Node1, [x,y,z,default]}, {Node2, [default]}]}]
%%
-spec reformat_capabilities(registered(),
                            [{node(), [{capability(), [mode()]}]}])
                           -> [{capability(), [{node(), [mode()]}]}].
reformat_capabilities(Registered, Capabilities) ->
    DefaultsL = [{Cap, [Info#capability.default]} || {Cap,Info} <- Registered],
    Defaults = orddict:from_list(DefaultsL),
    lists:foldl(fun({Node, NodeCaps}, Acc) ->
                        update_capability(Node, NodeCaps, Defaults, Acc)
                end, orddict:new(), Capabilities).

update_capability(Node, NodeCaps, Defaults, Acc0) ->
    NodeCaps2 = extend(orddict:from_list(NodeCaps), Defaults),
    lists:foldl(fun({Cap, Supported}, Acc) ->
                        S = ordsets:from_list(Supported),
                        orddict:append(Cap, {Node, S}, Acc)
                end, Acc0, NodeCaps2).

extend(A, B) ->
    orddict:merge(fun(_, L, X) -> X++L end, A, B).

%% For each capability, determine the modes supported by all nodes
-spec intersect_capabilities([{capability(), [{node(), [mode()]}]}])
                            -> [{capability(), [mode()]}].
intersect_capabilities(Capabilities) ->
    lists:map(fun intersect_supported/1, Capabilities).

intersect_supported({Capability, NodeSupported}) ->
    {_, Supported0} = hd(NodeSupported),
    Common =
        lists:foldl(fun({_Node, Supported}, Acc) ->
                            ordsets:intersection(Acc, Supported)
                    end, Supported0, tl(NodeSupported)),
    {Capability, Common}.

%% For each capability, re-order the computed mode list by local preference.
%% In reality, this is just an order-sensitive intersection between the local
%% node's list of supported modes and the computed list.
order_by_preference(MyCapabilities, Common) ->
    [order_by_preference(Cap, Pref, Common) || {Cap, Pref} <- MyCapabilities].

order_by_preference(Capability, Preferred, Common) ->
    Modes = orddict:fetch(Capability, Common),
    Preferred2 = [Mode || Mode <- Preferred,
                          lists:member(Mode, Modes)],
    {Capability, Preferred2}.

%% Override computed capabilities based on app.config settings
override_capabilities(Caps, AppOver) ->
    [override_capability(Cap, Modes, AppOver) || {Cap, Modes} <- Caps].

override_capability(Capability={App, CapName}, Modes, AppOver) ->
    Over = orddict:fetch(App, AppOver),
    case orddict:find(CapName, Over) of
        error ->
            {Capability, Modes};
        {ok, Opts} ->
            {Capability, override_capability(Opts, Modes)}
    end.

override_capability(Opts, Modes) ->
    Use = proplists:get_value(use, Opts),
    Prefer = proplists:get_value(prefer, Opts),
    case {Use, Prefer} of
        {undefined, undefined} ->
            Modes;
        {undefined, Val} ->
            case lists:member(Val, Modes) of
                true ->
                    [Val];
                false ->
                    Modes
            end;
        {Val, _} ->
            [Val]
    end.

get_overrides(Caps) ->
    Apps = lists:usort([App || {{App, _}, _} <- Caps]),
    AppOver = [{App, get_app_overrides(App)} || App <- Apps],
    AppOver.

get_app_overrides(App) ->
    case application:get_env(App, override_capability) of
        undefined ->
            [];
        {ok, L} ->
            orddict:from_list(L)
    end.

%% Log capability changes as well as update the capability ETS table.
%% The ETS table allows other processes to query current capabilities
%% without going through the capability server.
process_capability_changes(OldModes, NewModes) ->
    Diff = riak_core_util:orddict_delta(OldModes, NewModes),
    orddict:fold(fun(Capability, {'$none', New}, _) ->
                         ets:insert(?ETS, {Capability, New}),
                         lager:info("New capability: ~p = ~p", [Capability, New]);
                    (Capability, {Old, '$none'}, _) ->
                         ets:delete(?ETS, Capability),
                         lager:info("Removed capability ~p (previously: ~p)",
                                    [Capability, Old]);
                    (Capability, {Old, New}, _) ->
                         ets:insert(?ETS, {Capability, New}),
                         lager:info("Capability changed: ~p / ~p -> ~p",
                                    [Capability, Old, New])
                 end, ok, Diff).

%% Determine the capabilities supported by each cluster member based on the
%% information published in the ring
get_supported_from_ring(Ring) ->
    Members = riak_core_ring:all_members(Ring),
    [begin
         Caps = riak_core_ring:get_member_meta(Ring, Member, ?CAPS),
         case Caps of
             undefined ->
                 {Member, []};
             _ ->
                 {Member, Caps}
         end
     end || Member <- Members].

%% Determine capabilities of legacy nodes based on app.config settings and
%% the provided app-var -> mode mapping associated with capabilities when
%% registered.
query_capabilities(Node, State=#state{registered=Registered}) ->
    %% Only query results we do not already have local knowledge of
    Known = dict:from_list(get_supported(Node, State)),
    lists:mapfoldl(fun({Capability, Info}, ResolvedAcc) ->
                           {Resv, Cap} = query_capability(Node,
                                                          Known,
                                                          Capability,
                                                          Info#capability.default,
                                                          Info#capability.legacy),
                           {Cap, ResolvedAcc and Resv}
                   end, true, Registered).

query_capability(Node, Known, Capability, DefaultSup, LegacyVar) ->
    case dict:find(Capability, Known) of
        {ok, Supported} ->
            {true, {Capability, Supported}};
        error ->
            query_capability(Node, Capability, DefaultSup, LegacyVar)
    end.

query_capability(_, Capability, DefaultSup, undefined) ->
    Default = {Capability, [DefaultSup]},
    {true, Default};
query_capability(Node, Capability, DefaultSup, {App, Var, Map}) ->
    Default = {Capability, [DefaultSup]},
    Result = riak_core_util:safe_rpc(Node, application, get_env, [App, Var]),
    case Result of
        {badrpc, _} ->
            {false, Default};
        undefined ->
            {true, Default};
        {ok, Value} ->
            case lists:keyfind(Value, 1, Map) of
                false ->
                    {true, Default};
                {Value, Supported} ->
                    {true, {Capability, [Supported]}}
            end
    end.

save_registered(Registered) ->
    application:set_env(riak_core, registered_capabilities, Registered).

load_registered() ->
    case application:get_env(riak_core, registered_capabilities) of
        undefined -> [];
        {ok, Caps} -> Caps
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

basic_test() ->
    S1 = init_state([]),

    S2 = register_capability(n1, 
                             {riak_core, test},
                             capability_info([x,a,c,y], y, []),
                             S1),
    S3 = add_node_capabilities(n2,
                               [{{riak_core, test}, [a,b,c,y]}],
                               S2),
    S4 = negotiate_capabilities(n1, [{riak_core, []}], S3),
    ?assertEqual([{{riak_core, test}, a}], S4#state.negotiated),

    S5 = negotiate_capabilities(n1,
                                [{riak_core, [{test, [{prefer, c}]}]}],
                                S4),
    ?assertEqual([{{riak_core, test}, c}], S5#state.negotiated),

    S6 = add_node_capabilities(n3,
                               [{{riak_core, test}, [b]}],
                               S5),
    S7 = negotiate_capabilities(n1, [{riak_core, []}], S6),
    ?assertEqual([{{riak_core, test}, y}], S7#state.negotiated),

    S8 = negotiate_capabilities(n1,
                                [{riak_core, [{test, [{use, x}]}]}],
                                S7),
    ?assertEqual([{{riak_core, test}, x}], S8#state.negotiated),
    ok.

-endif.
