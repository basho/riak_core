%%% File        : broadcast_eqc.erl
%%% Author      : Ulf Norell
%%% Description : 
%%% Created     : 20 Aug 2013 by Ulf Norell
-module(broadcast_eqc).

-compile(export_all).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eqc/include/eqc_temporal.hrl").

-record(state, {nodes = []}).
-record(node, {name, context}).

-define(LAZY_TIMER, 20).

initial_state() -> #state{}.

node_list() ->
  [a, b, c, d, e, f, g, h, i, j, k, l].

%% -- Generators -------------------------------------------------------------

key() -> elements([k1, k2, k3, k4, k5]).
val() -> elements([v1, v2, v3, v4, v5]).
msg() -> {key(), val()}.

gen_peers(Node, Nodes) ->
  case length(Nodes) of
    1 -> {[], []};
    2 -> {Nodes -- [Node], []};
    N ->
      Width = if N < 5 -> 1; true -> 2 end,
      Tree  = riak_core_util:build_tree(Width, Nodes, [cycles]),
      Eager = orddict:fetch(Node, Tree),
      %% Lazy  = [elements(Nodes -- [Node | Eager])],
      Xs = Nodes, %  -- (Eager -- [Node]),
      {_, [_,X|_]} = lists:splitwith(fun(X) -> X /= Node end, Xs ++ Xs),
      Lazy = [X],
      {Eager, Lazy}
  end.

drop_strategy(Nodes) when length(Nodes) < 2 -> [];
drop_strategy(Nodes) ->
  ?LET(Strat, list({{elements(Nodes), elements(Nodes)}, ?LET(Xs, list(nat()), mk_drop_list(keep, drop, Xs))}),
    [ E || E={_, Xs} <- Strat, [] /= Xs ]).
  %% [].

mk_drop_list(_, _, [])     -> [];
mk_drop_list(keep, _, [_]) -> [];
mk_drop_list(A, B, [N|Ns]) ->
  [{A, N} || N > 0] ++ mk_drop_list(B, A, Ns).

%% -- Metadata manager abstraction -------------------------------------------

-define(MOCK_MANAGER, true).

-ifdef(MOCK_MANAGER).

-define(MANAGER, metadata_manager_mock).

start_manager(_Node) ->
  (catch ?MANAGER:stop()),
  timer:sleep(1),
  {ok, Mgr} = ?MANAGER:start_link(),
  unlink(Mgr).

mk_key(Key) -> Key.

put_arguments(_Name, Key, _Context, Val) ->
  [Key, Val].

broadcast_obj(Key, Val) -> {Key, Val}.

get_view(Node) ->
  rpc:call(mk_node(Node), ?MANAGER, stop, []).

-else.

-define(MANAGER, riak_core_metadata_manager).
-define(PREFIX, {x, x}).

-include("../include/riak_core_metadata.hrl").

start_manager(Node) ->
  Dir = atom_to_list(Node),
  os:cmd("mkdir " ++ Dir),
  os:cmd("rm " ++ Dir ++ "/*"),
  (catch exit(whereis(?MANAGER), kill)),
  timer:sleep(1),
  {ok, Mgr} = ?MANAGER:start_link([{data_dir, Dir}, {node_name, Node}]),
  unlink(Mgr).

mk_key(Key) -> {?PREFIX, Key}.  %% TODO: prefix

put_arguments(_Name, Key, Context, Val) ->
  [Key, Context, Val].

broadcast_obj(Key, Val) ->
  #metadata_broadcast{ pkey = Key, obj = Val }.

get_view(Node) ->
  rpc:call(mk_node(Node), ?MODULE, get_view, []).

get_view() ->
  It = ?MANAGER:iterator(?PREFIX, '_'),
  iterate(It, []).

iterate(It, Acc) ->
  case ?MANAGER:iterator_done(It) of
    true  -> lists:reverse(Acc);
    false -> iterate(?MANAGER:iterate(It), [?MANAGER:iterator_value(It)|Acc])
  end.
-endif.

%% -- Commands ---------------------------------------------------------------

%% -- init --
init_pre(S) -> S#state.nodes == [].

init_pre(_S, [Nodes, _]) -> length(Nodes) > 2.

init_args(_S) ->
  ?LET(Nodes, ?LET(N, choose(3, length(node_list())),
                    shrink_list(lists:sublist(node_list(), N))),
  [ [ {Node, gen_peers(Node, Nodes)} || Node <- Nodes ]
  , drop_strategy(Nodes)
  ]).

init(Tree, DropStrat) ->
  Names = [ Name || {Name, _} <- Tree ],
  [ rpc:call(mk_node(Name), ?MODULE, start_server, [Name, Eager, Lazy, Names])
    || {Name, {Eager, Lazy}} <- Tree ],
  proxy_server:setup(DropStrat),
  ok.

start_server(Node, Eager, Lazy, Names) ->
  try
    N = fun(Xs) -> lists:map(fun mk_node/1, Xs) end,
    {ok, Pid} = riak_core_broadcast:start_link(N(Names), N(Eager), N(Lazy)),
    unlink(Pid),
    start_manager(Node),
    ok
  catch _:Err ->
    io:format("OOPS\n~p\n~p\n", [Err, erlang:get_stacktrace()])
  end.

init_next(S, _, [Names, _]) ->
  S#state{ nodes  = [ #node{ name = Name } || {Name, _} <- Names ] }.

%% -- broadcast --
broadcast_pre(S) -> S#state.nodes /= [].
broadcast_pre(S, [Node, _, _, _]) -> lists:keymember(Node, #node.name, S#state.nodes).

broadcast_args(S) ->
  ?LET({{Key, Val}, #node{name = Name, context = Context}},
       {msg(), elements(S#state.nodes)},
    [Name, Key, Val, Context]).

broadcast(Node, Key0, Val0, Context) ->
  Key = mk_key(Key0),
  event_logger:event({put, Node, Key0, Val0, Context}),
  Val = rpc:call(mk_node(Node), ?MANAGER, put, put_arguments(Node, Key, Context, Val0)),
  rpc:call(mk_node(Node), riak_core_broadcast, broadcast, [broadcast_obj(Key, Val), ?MANAGER]).
    %% Parameterize on manager

broadcast_next(S, Context, [Node, _Key, _Val, _Context]) ->
  S#state{ nodes = lists:keystore(Node, #node.name, S#state.nodes,
                                  #node{ name = Node, context = Context }) }.

%% -- sleep --
sleep_pre(S) -> S#state.nodes /= [].
sleep_args(_) -> [choose(1, ?LAZY_TIMER)].
sleep(N) -> timer:sleep(N).

weight(_, sleep)     -> 1;
weight(_, broadcast) -> 4;
weight(_, init)      -> 1.

%% -- Property ---------------------------------------------------------------

prop_test() ->
  ?SETUP(fun() -> setup(), fun() -> ok end end,
  ?FORALL(Cmds, ?SIZED(N, resize(N div 2, commands(?MODULE))),
  ?LET(Shrinking, parameter(shrinking, false),
  ?ALWAYS(if Shrinking -> 1; true -> 1 end,
  begin
    timer:sleep(2),
    event_logger:reset(),
    HSR={_H, S, _Res} = run_commands(?MODULE, Cmds),
    %% timer:sleep(200),
    {Trace, Ok} = event_logger:get_events(300, 10000),
    event_logger:event(reset),
    {messages, Mailbox} = process_info(global:whereis_name(event_logger), messages),
    timer:sleep(10),
    Tree = (catch get_tree(S#state.nodes)),
    stop_servers(),
    timer:sleep(5),
    Views = [ {Node, get_view(Node)} || #node{name = Node} <- S#state.nodes ],
    timer:sleep(10),
    MoreTrace = event_logger:get_events(),
    ?IMPLIES(length(MoreTrace) == 2,
    aggregate([ element(1, E) || {_, E} <- Trace, is_tuple(E) ],
    ?WHENFAIL(io:format("~p\n(length: ~p)\n", [Trace, length(Trace)]),
    ?WHENFAIL(io:format("Views =\n  ~p\n", [Views]),
    ?WHENFAIL(io:format("MoreTrace =\n  ~p\nTree =\n  ~p\nMailbox =\n  ~p\n", [MoreTrace, Tree, Mailbox]),
    pretty_commands(?MODULE, Cmds, HSR,
      conjunction(
      [ {consistent, prop_consistent(Views)}
      , {valid_views, [] == [ bad || {_, View} <- Views, not is_list(View) ]}
      , {termination, equals(Ok, ok)}
      %% , {lazy_sets, prop_tree(Tree)}
      %% , {extra_trace, equals(length(MoreTrace), 2)}
      ])))))))
  end)))).

prop_tree(Tree) ->
  Nodes = [ Node || {Node, _} <- Tree ],
  conjunction(
    [ {Node, equals(Nodes -- reachable(Node, Tree), [])}
      || Node <- Nodes ]).

reachable(Node, Tree) -> reachable([Node], Node, Tree, []).
reachable([], _Root, _Tree, Acc) -> lists:sort(Acc);
reachable([Node|Nodes], Root, Tree, Acc) ->
  case lists:member(Node, Acc) of
    true -> reachable(Nodes, Root, Tree, Acc);
    false -> 
      {_, _, Neighbours} = lists:keyfind(Root, 1, proplists:get_value(Node, Tree)),
      reachable(Nodes ++ Neighbours, Root, Tree, [Node|Acc])
  end.

prop_consistent([]) -> true;
prop_consistent(Views) ->
  Dicts = [ Dict || {_, Dict} <- Views ],
  1 == length(lists:usort(Dicts)).

setup() ->
  %% error_logger:tty(false),
  error_logger:tty(true),
  (catch proxy_server:start_link()),
  try event_logger:get_events() catch _:_ -> event_logger:start() end,
  start_nodes(),
  [ rpc:call(mk_node(Node), application, set_env,
      [riak_core, broadcast_lazy_timer, ?LAZY_TIMER])
    || Node <- node_list() ].

where_is_event_logger() ->
  io:format("event_logger: ~p\n", [global:whereis_name(event_logger)]).

%% -- Helpers ----------------------------------------------------------------

host() ->
  hd(tl(string:tokens(atom_to_list(node()),"@"))).

mk_node(Name) ->
  list_to_atom(lists:concat([Name, "@", host()])).

node_name() ->
  node_name(node()).

node_name(Node) ->
  list_to_atom(hd(string:tokens(atom_to_list(Node),"@"))).

start_nodes() ->
  [ start_node(Node) || Node <- node_list() ].

stop_nodes() ->
  [ slave:stop(mk_node(Node)) || Node <- node_list() ].

start_node(Node) ->
  case lists:member(mk_node(Node), nodes()) of
    true  ->
      rpc:call(mk_node(Node), user_default, l, []);
    false ->
      {ok, _} = slave:start(host(), Node),
      rpc:call(mk_node(Node), global, sync, [])
  end.

kill(Name) ->
  catch exit(whereis(Name), kill).

stop_servers() ->
  [ rpc:call(mk_node(P), ?MODULE, kill, [riak_core_broadcast])
    || P <- node_list() ].

proplists_modify(Key, List, Fun) ->
  Val = proplists:get_value(Key, List),
  lists:keystore(Key, 1, List, {Key, Fun(Val)}).

get_tree(Nodes) ->
  [ {A, [ begin
            {Eager, Lazy} = riak_core_broadcast:debug_get_peers(mk_node(A), mk_node(B)),
            {B, lists:map(fun node_name/1, Eager), lists:map(fun node_name/1, Lazy)}
          end || #node{name = B} <- Nodes ]}
    || #node{name = A} <- Nodes ].

%% prop_send_after() ->
%%   ?FORALL(N, choose(10, 40),
%%   begin
%%     T0 = timestamp(),
%%     erlang:send_after(N, self(), done),
%%     T1 = receive done -> timestamp() end,
%%     ?WHENFAIL(io:format("T0 = ~p\nT1 = ~p\nD  = ~p\n", [T0, T1, T1 - T0]),
%%     abs(T1 - T0 - N) < 4)
%%   end).

%% timestamp() -> from_now(os:timestamp()).

%% from_now({A, B, C}) ->
%%   (C + 1000000 * (B + 1000000 * A)) div 1000.

