%%% File        : riak_core_broadcast_eqc.erl
%%% Author      : Ulf Norell
%%% Description : 
%%% Created     : 19 Aug 2013 by Ulf Norell
-module(riak_core_broadcast_eqc).

-compile(export_all).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_component.hrl").

-record(state, {node, members, eager_sets, lazy_sets, lazy_queue = []}).

initial_state() -> #state{ members = node_list() }.

%% -- Generators -------------------------------------------------------------

node_list() ->
  [a, b, c, d, e, f, g, h]. % , i, j, k, l].

peers(Node) ->
  ?LET(N, choose(1, 5),   %% TODO: singleton cluster?
  ?LET(I, choose(1, N),
    lists:split(I, lists:sublist(lists:delete(Node, node_list()), N))
  )).

msg() ->
  elements([ping, put, get]).

msg_id() ->
  nat().

msg_body() ->
  elements([bla, foo, bar]).

nodename(Self) -> elements(node_list() -- [Self]).

%% -- Commands ---------------------------------------------------------------

eager_peers(S, Root) ->
  proplists:get_value(Root, S#state.eager_sets).

lazy_peers(S, Root) ->
  proplists:get_value(Root, S#state.lazy_sets).

%% -- init --
init_args(_) ->
  ?LET(Node, elements(node_list()),
  [Node, peers(Node)]).

init_pre(S) -> S#state.node == undefined.

init(Node, {Eager, Lazy}) ->
  Peers = node_list() -- [Node],
  Q = fun(Ns) -> lists:map(fun slave_node/1, Ns) end,
  {ok, Pid} = rpc:call(slave_node(Node), riak_core_broadcast, start_link, [Q(Peers), Q(Eager), Q(Lazy)]),
  unlink(Pid),
  [ begin
      {ok, P} = rpc:call(slave_node(Peer), riak_core_broadcast_peer, start_link, [node(), Peer]),
      unlink(P)
    end || Peer <- Peers ],
  ok.

init_next(S, _, [Node, {Eager, Lazy}]) ->
  S#state{ node = Node, eager_sets = [{Root, Eager} || Root <- node_list() ],
                        lazy_sets  = [{Root, Lazy}  || Root <- node_list() ] }.

%% -- broadcast --
broadcast_args(S) ->
  oneof(
  [ [S#state.node, msg()]
  , [S#state.node, msg_id(), msg_body(), nat(), nodename(S#state.node), nodename(S#state.node)]
  ]).

broadcast_pre(S) -> S#state.node /= undefined.

broadcast_pre(S, [Node|_]) -> S#state.node == Node.

broadcast(Node, Msg) ->
  X = rpc:call(slave_node(Node), riak_core_broadcast, broadcast, [Msg, ?MODULE]),
  timer:sleep(30),
  X.

broadcast(Node, Id, Body, Round, Root, From) ->
  X = rpc:call(slave_node(Node), gen_server, cast,
        [riak_core_broadcast, {broadcast, Id, Body, ?MODULE, Round, slave_node(Root), slave_node(From)}]),
  timer:sleep(30),
  X.

broadcast_callouts(_S, [Node, Msg]) ->
  ?BIND({Id, Body}, ?CALLOUT(data, broadcast_data, [Msg], {msg_id(), msg_body()}),
  ?SEQ(?SELFCALL(eager_push, [Node, Id, Body, 0, Node, Node]),
       ?SELFCALL(lazy_push,  [Node, Id, Body, 0, Node, Node])));

broadcast_callouts(__S, [Node, Id, Body, Round, Root, From]) ->
  ?BIND(Valid, ?CALLOUT(data, merge, [Id, Body], weighted_default({3, true}, {1, false})),
  case Valid of
    true ->
      ?SEQ([?SELFCALL(eager_push, [Node, Id, Body, Round + 1, Root, From]),
            ?SELFCALL(lazy_push,  [Node, Id, Body, Round + 1, Root, From]),
            ?SELFCALL(make_eager, [Root, From])]);
    false ->
      ?SEQ(?SELFCALL(make_lazy, [Root, From]),
           ?CALLOUT(peer, cast, [From, {prune, slave_node(Root), slave_node(Node)}], ok))
  end).

%% -- lazy_tick --

lazy_tick_args(S) -> [S#state.node].

lazy_tick_pre(S)         -> S#state.node /= undefined.
lazy_tick_pre(S, [Node]) -> S#state.node == Node.

lazy_tick(Node) ->
  X = rpc:call(slave_node(Node), erlang, send, [riak_core_broadcast, lazy_tick]),
  timer:sleep(30),
  X.

lazy_tick_callouts(S, [_Node]) ->
  ?PAR([ ?CALLOUT(peer, cast, [P, Msg], ok) || {P, Msg} <- S#state.lazy_queue ]).

% -- Internal callouts ------------------------------------------------------

eager_push_callouts(S, [Node, Id, Body, Round, Root, From]) ->
  ?PAR([ ?CALLOUT(peer, cast, [P, {broadcast, Id, Body, ?MODULE, Round, slave_node(Root), slave_node(Node)}], ok)
        || P <- eager_peers(S, Root), P /= From ]).

lazy_push_next(S, _, [Node, Id, _Body, Round, Root, From]) ->
  S#state{ lazy_queue = lists:umerge(S#state.lazy_queue,
            lists:sort([ {P, {i_have, Id, ?MODULE, Round, slave_node(Root), slave_node(Node)}}
                        || P <- lazy_peers(S, Root), P /= From ])) }.

dispatch_callouts(_S, []) -> ?EMPTY.

make_lazy_next(S, _, [Root, P]) ->
  S#state{ eager_sets = proplists_modify(Root, S#state.eager_sets, fun(Eager) -> Eager -- [P] end)
         , lazy_sets  = proplists_modify(Root, S#state.lazy_sets,  fun(Lazy)  -> (Lazy -- [P]) ++ [P] end)
         }.

make_eager_next(S, _, [Root, P]) ->
  S#state{ eager_sets = proplists_modify(Root, S#state.eager_sets, fun(Eager) -> (Eager -- [P]) ++ [P] end)
         , lazy_sets  = proplists_modify(Root, S#state.lazy_sets,  fun(Lazy)  -> Lazy   -- [P] end)
         }.

%% -- Property ---------------------------------------------------------------

prop_test() ->
  ?SETUP(fun() -> setup(), fun() -> ok end end,
  ?FORALL(Cmds, commands(?MODULE),
  begin
    timer:sleep(2),
    HSR={_H, S, Res} = run_commands(?MODULE, Cmds),
    stop_peers(S),
    pretty_commands(?MODULE, Cmds, HSR,
      Res == ok)
  end)).

api_spec() ->
  #api_spec{
    modules = [
      #api_module{
        name = data,
        functions = [
          #api_fun{ name = broadcast_data, arity = 1 },
          #api_fun{ name = merge, arity = 2 }
        ]
      },
      #api_module{
        name = peer,
        functions = [
          #api_fun{ name = call, arity = 2 },
          #api_fun{ name = cast, arity = 2 },
          #api_fun{ name = info, arity = 2 }
        ]
      }]}.

%% -- Stuff ------------------------------------------------------------------

kill(Name) ->
  catch exit(whereis(Name), kill).

setup() ->
  error_logger:tty(false),
  eqc_mocking:start_mocking(api_spec()),
  start_nodes(),
  [ rpc:call(slave_node(Node), application, set_env,
      [riak_core, broadcast_lazy_timer, 4294967295])
    || Node <- node_list() ].

host() ->
  hd(tl(string:tokens(atom_to_list(node()),"@"))).

slave_node(Name) ->
  list_to_atom(lists:concat([Name, "@", host()])).

start_nodes() ->
  [ start_node(Node) || Node <- node_list() ].

start_node(Node) ->
  case lists:member(slave_node(Node), nodes()) of
    true  ->
      rpc:call(slave_node(Node), user_default, l, []);
    false ->
      {ok, _} = slave:start(host(), Node),
      rpc:call(slave_node(Node), ?MODULE, register_root, [node()]),
      ok
  end.

stop_peers(#state{ node = undefined }) -> ok;
stop_peers(S) ->
  rpc:call(slave_node(S#state.node), ?MODULE, kill, [riak_core_broadcast]),
  [ rpc:call(slave_node(P), riak_core_broadcast_peer, stop, [])
    || P <- S#state.members ].

register_root(Node) ->
  file:write_file(".root", io_lib:format("~p.\n", [Node])).

root_node() ->
  {ok, [Node]} = file:consult(".root"),
  Node.

proplists_modify(Key, List, Fun) ->
  Val = proplists:get_value(Key, List),
  lists:keystore(Key, 1, List, {Key, Fun(Val)}).

%% -- Mocked functions -------------------------------------------------------

broadcast_data(Msg) ->
  rpc:call(root_node(), data, broadcast_data, [Msg]).

merge(Id, Body) ->
  rpc:call(root_node(), data, merge, [Id, Body]).

