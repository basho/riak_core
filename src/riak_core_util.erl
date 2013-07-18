%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Various functions that are useful throughout Riak.
-module(riak_core_util).

-export([moment/0,
         make_tmp_dir/0,
         replace_file/2,
         compare_dates/2,
         reload_all/1,
         integer_to_list/2,
         unique_id_62/0,
         str_to_node/1,
         chash_key/1,
         chash_std_keyfun/1,
         chash_bucketonly_keyfun/1,
         mkclientid/1,
         start_app_deps/1,
         build_tree/3,
         orddict_delta/2,
         rpc_every_member/4,
         rpc_every_member_ann/4,
         pmap/2,
         pmap/3,
         multi_rpc/4,
         multi_rpc/5,
         multi_rpc_ann/4,
         multi_rpc_ann/5,
         multicall_ann/4,
         multicall_ann/5,
         shuffle/1,
         is_arch/1,
         format_ip_and_port/2,
         peername/2,
         sockname/2
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([counter_loop/1,incr_counter/1,decr_counter/1]).
-endif.

%% R14 Compatibility
-compile({no_auto_import,[integer_to_list/2]}).

%% ===================================================================
%% Public API
%% ===================================================================

%% @spec moment() -> integer()
%% @doc Get the current "moment".  Current implementation is the
%%      number of seconds from year 0 to now, universal time, in
%%      the gregorian calendar.
moment() -> calendar:datetime_to_gregorian_seconds(calendar:universal_time()).

%% @spec compare_dates(string(), string()) -> boolean()
%% @doc Compare two RFC1123 date strings or two now() tuples (or one
%%      of each).  Return true if date A is later than date B.
compare_dates(A={_,_,_}, B={_,_,_}) ->
    %% assume 3-tuples are now() times
    A > B;
compare_dates(A, B) when is_list(A) ->
    %% assume lists are rfc1123 date strings
    compare_dates(rfc1123_to_now(A), B);
compare_dates(A, B) when is_list(B) ->
    compare_dates(A, rfc1123_to_now(B)).

%% 719528 days from Jan 1, 0 to Jan 1, 1970
%%  *86400 seconds/day
-define(SEC_TO_EPOCH, 62167219200).

rfc1123_to_now(String) when is_list(String) ->
    GSec = calendar:datetime_to_gregorian_seconds(
             httpd_util:convert_request_date(String)),
    ESec = GSec-?SEC_TO_EPOCH,
    Sec = ESec rem 1000000,
    MSec = ESec div 1000000,
    {MSec, Sec, 0}.

%% @spec make_tmp_dir() -> string()
%% @doc Create a unique directory in /tmp.  Returns the path
%%      to the new directory.
make_tmp_dir() ->
    TmpId = io_lib:format("riptemp.~p",
                          [erlang:phash2({random:uniform(),self()})]),
    TempDir = filename:join("/tmp", TmpId),
    case filelib:is_dir(TempDir) of
        true -> make_tmp_dir();
        false ->
            ok = file:make_dir(TempDir),
            TempDir
    end.

-spec replace_file(string(), iodata()) -> ok | {error, term()}.

replace_file(FN, Data) ->
    TmpFN = FN ++ ".tmp",
    {ok, FH} = file:open(TmpFN, [write, raw]),
    try
        ok = file:write(FH, Data),
        ok = file:sync(FH),
        ok = file:close(FH),
        ok = file:rename(TmpFN, FN),
        {ok, Contents} = read_file(FN),
        true = (Contents == iolist_to_binary(Data)),
        ok
    catch _:Err ->
            {error, Err}
    end.

%% @doc Similar to {@link file:read_file} but uses raw file I/O
read_file(FName) ->
    {ok, FD} = file:open(FName, [read, raw, binary]),
    IOList = read_file(FD, []),
    file:close(FD),
    {ok, iolist_to_binary(IOList)}.

read_file(FD, Acc) ->
    case file:read(FD, 4096) of
        {ok, Data} ->
            read_file(FD, [Data|Acc]);
        eof ->
            lists:reverse(Acc)
    end.

%% @spec integer_to_list(Integer :: integer(), Base :: integer()) ->
%%          string()
%% @doc Convert an integer to its string representation in the given
%%      base.  Bases 2-62 are supported.
integer_to_list(I, 10) ->
    erlang:integer_to_list(I);
integer_to_list(I, Base)
  when is_integer(I), is_integer(Base),Base >= 2, Base =< 1+$Z-$A+10+1+$z-$a ->
    if I < 0 ->
            [$-|integer_to_list(-I, Base, [])];
       true ->
            integer_to_list(I, Base, [])
    end;
integer_to_list(I, Base) ->
    erlang:error(badarg, [I, Base]).

%% @spec integer_to_list(integer(), integer(), string()) -> string()
integer_to_list(I0, Base, R0) ->
    D = I0 rem Base,
    I1 = I0 div Base,
    R1 = if D >= 36 ->
		 [D-36+$a|R0];
	    D >= 10 ->
		 [D-10+$A|R0];
	    true ->
		 [D+$0|R0]
	 end,
    if I1 =:= 0 ->
	    R1;
       true ->
	    integer_to_list(I1, Base, R1)
    end.

-ifndef(old_hash).
sha(Bin) ->
    crypto:hash(sha, Bin).
-else.
sha(Bin) ->
    crypto:sha(Bin).
-endif.

%% @spec unique_id_62() -> string()
%% @doc Create a random identifying integer, returning its string
%%      representation in base 62.
unique_id_62() ->
    Rand = sha(term_to_binary({make_ref(), os:timestamp()})),
    <<I:160/integer>> = Rand,
    integer_to_list(I, 62).

%% @spec reload_all(Module :: atom()) ->
%%         [{purge_response(), load_file_response()}]
%% @type purge_response() = boolean()
%% @type load_file_response() = {module, Module :: atom()}|
%%                              {error, term()}
%% @doc Ask each member node of the riak ring to reload the given
%%      Module.  Return is a list of the results of code:purge/1
%%      and code:load_file/1 on each node.
reload_all(Module) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    [{rpc:call(Node, code, purge, [Module]),
     rpc:call(Node, code, load_file, [Module])} ||
        Node <- riak_core_ring:all_members(Ring)].

%% @spec mkclientid(RemoteNode :: term()) -> ClientID :: list()
%% @doc Create a unique-enough id for vclock clients.
mkclientid(RemoteNode) ->
    {{Y,Mo,D},{H,Mi,S}} = erlang:universaltime(),
    {_,_,NowPart} = os:timestamp(),
    Id = erlang:phash2([Y,Mo,D,H,Mi,S,node(),RemoteNode,NowPart,self()]),
    <<Id:32>>.

%% @spec chash_key(BKey :: riak_object:bkey()) -> chash:index()
%% @doc Create a binary used for determining replica placement.
chash_key({Bucket,Key}) ->
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    {chash_keyfun, {M, F}} = proplists:lookup(chash_keyfun, BucketProps),
    M:F({Bucket,Key}).

%% @spec chash_std_keyfun(BKey :: riak_object:bkey()) -> chash:index()
%% @doc Default object/ring hashing fun, direct passthrough of bkey.
chash_std_keyfun({Bucket, Key}) -> chash:key_of({Bucket, Key}).

%% @spec chash_bucketonly_keyfun(BKey :: riak_object:bkey()) -> chash:index()
%% @doc Object/ring hashing fun that ignores Key, only uses Bucket.
chash_bucketonly_keyfun({Bucket, _Key}) -> chash:key_of(Bucket).

str_to_node(Node) when is_atom(Node) ->
    str_to_node(atom_to_list(Node));
str_to_node(NodeStr) ->
    case string:tokens(NodeStr, "@") of
        [NodeName] ->
            %% Node name only; no host name. If the local node has a hostname,
            %% append it
            case node_hostname() of
                [] ->
                    list_to_atom(NodeName);
                Hostname ->
                    list_to_atom(NodeName ++ "@" ++ Hostname)
            end;
        _ ->
            list_to_atom(NodeStr)
    end.

node_hostname() ->
    NodeStr = atom_to_list(node()),
    case string:tokens(NodeStr, "@") of
        [_NodeName, Hostname] ->
            Hostname;
        _ ->
            []
    end.

%% @spec start_app_deps(App :: atom()) -> ok
%% @doc Start depedent applications of App.
start_app_deps(App) ->
    {ok, DepApps} = application:get_key(App, applications),
    [ensure_started(A) || A <- DepApps],
    ok.
    

%% @spec ensure_started(Application :: atom()) -> ok
%% @doc Start the named application if not already started.
ensure_started(App) ->
    case application:start(App) of
	ok ->
	    ok;
	{error, {already_started, App}} ->
	    ok
    end.

%% @doc Invoke function `F' over each element of list `L' in parallel,
%%      returning the results in the same order as the input list.
-spec pmap(function(), [node()]) -> [any()].
pmap(F, L) ->
    Parent = self(),
    lists:foldl(
      fun(X, N) ->
              spawn(fun() ->
                            Parent ! {pmap, N, F(X)}
                    end),
              N+1
      end, 0, L),
    L2 = [receive {pmap, N, R} -> {N,R} end || _ <- L],
    {_, L3} = lists:unzip(lists:keysort(1, L2)),
    L3.

-record(pmap_acc,{
                  mapper,
                  fn,
                  n_pending=0,
                  pending=sets:new(),
                  n_done=0,
                  done=[],
                  max_concurrent=1
                  }).

%% @doc Parallel map with a cap on the number of concurrent worker processes.
%% Note: Worker processes are linked to the parent, so a crash propagates.
-spec pmap(Fun::function(), List::list(), MaxP::integer()) -> list().
pmap(Fun, List, MaxP) when MaxP < 1 ->
    pmap(Fun, List, 1);
pmap(Fun, List, MaxP) when is_function(Fun), is_list(List), is_integer(MaxP) ->
    Mapper = self(),
    #pmap_acc{pending=Pending, done=Done} =
                 lists:foldl(fun pmap_worker/2,
                             #pmap_acc{mapper=Mapper,
                                       fn=Fun,
                                       max_concurrent=MaxP},
                             List),
    All = pmap_collect_rest(Pending, Done),
    % Restore input order
    Sorted = lists:keysort(1, All),
    [ R || {_, R} <- Sorted ].

%% @doc Fold function for {@link pmap/3} that spawns up to a max number of
%% workers to execute the mapping function over the input list.
pmap_worker(X, Acc = #pmap_acc{n_pending=NP,
                               pending=Pending,
                               n_done=ND,
                               max_concurrent=MaxP,
                               mapper=Mapper,
                               fn=Fn})
  when NP < MaxP ->
    Worker =
        spawn_link(fun() ->
                           R = Fn(X),
                           Mapper ! {pmap_result, self(), {NP+ND, R}}
                   end),
    Acc#pmap_acc{n_pending=NP+1, pending=sets:add_element(Worker, Pending)};
pmap_worker(X, Acc = #pmap_acc{n_pending=NP,
                               pending=Pending,
                               n_done=ND,
                               done=Done,
                               max_concurrent=MaxP})
  when NP == MaxP ->
    {Result, NewPending} = pmap_collect_one(Pending),
    pmap_worker(X, Acc#pmap_acc{n_pending=NP-1, pending=NewPending,
                                n_done=ND+1, done=[Result|Done]}).

%% @doc Waits for one pending pmap task to finish
pmap_collect_one(Pending) ->
    receive
        {pmap_result, Pid, Result} ->
            Size = sets:size(Pending),
            NewPending = sets:del_element(Pid, Pending),
            case sets:size(NewPending) of
                Size ->
                    pmap_collect_one(Pending);
                _ ->
                    {Result, NewPending}
            end
    end.

pmap_collect_rest(Pending, Done) ->
    case sets:size(Pending) of
        0 ->
            Done;
        _ ->
            {Result, NewPending} = pmap_collect_one(Pending),
            pmap_collect_rest(NewPending, [Result | Done])
    end.


%% @spec rpc_every_member(atom(), atom(), [term()], integer()|infinity)
%%          -> {Results::[term()], BadNodes::[node()]}
%% @doc Make an RPC call to the given module and function on each
%%      member of the cluster.  See rpc:multicall/5 for a description
%%      of the return value.
rpc_every_member(Module, Function, Args, Timeout) ->
    {ok, MyRing} = riak_core_ring_manager:get_my_ring(),
    Nodes = riak_core_ring:all_members(MyRing),
    rpc:multicall(Nodes, Module, Function, Args, Timeout).

%% @doc Same as rpc_every_member/4, but annotate the result set with
%%      the name of the node returning the result.
-spec rpc_every_member_ann(module(), atom(), [term()], integer()|infinity)
                          -> {Results::[{node(), term()}], Down::[node()]}.
rpc_every_member_ann(Module, Function, Args, Timeout) ->
    {ok, MyRing} = riak_core_ring_manager:get_my_ring(),
    Nodes = riak_core_ring:all_members(MyRing),
    {Results, Down} = multicall_ann(Nodes, Module, Function, Args, Timeout),
    {Results, Down}.

%% @doc Perform an RPC call to a list of nodes in parallel, returning the
%%      results in the same order as the input list.
-spec multi_rpc([node()], module(), function(), [any()]) -> [any()].
multi_rpc(Nodes, Mod, Fun, Args) ->
    multi_rpc(Nodes, Mod, Fun, Args, infinity).

%% @doc Perform an RPC call to a list of nodes in parallel, returning the
%%      results in the same order as the input list.
-spec multi_rpc([node()], module(), function(), [any()], timeout()) -> [any()].
multi_rpc(Nodes, Mod, Fun, Args, Timeout) ->
    pmap(fun(Node) ->
                 rpc:call(Node, Mod, Fun, Args, Timeout)
         end, Nodes).

%% @doc Perform an RPC call to a list of nodes in parallel, returning the
%%      results in the same order as the input list. Each result is tagged
%%      with the corresponding node name.
-spec multi_rpc_ann([node()], module(), function(), [any()])
                   -> [{node(), any()}].
multi_rpc_ann(Nodes, Mod, Fun, Args) ->
    multi_rpc_ann(Nodes, Mod, Fun, Args, infinity).

%% @doc Perform an RPC call to a list of nodes in parallel, returning the
%%      results in the same order as the input list. Each result is tagged
%%      with the corresponding node name.
-spec multi_rpc_ann([node()], module(), function(), [any()], timeout())
                   -> [{node(), any()}].
multi_rpc_ann(Nodes, Mod, Fun, Args, Timeout) ->
    Results = multi_rpc(Nodes, Mod, Fun, Args, Timeout),
    lists:zip(Nodes, Results).

%% @doc Similar to {@link rpc:multicall/4}. Performs an RPC call to a list
%%      of nodes in parallel, returning a list of results as well as a list
%%      of nodes that are down/unreachable. The results will be returned in
%%      the same order as the input list, and each result is tagged with the
%%      corresponding node name.
-spec multicall_ann([node()], module(), function(), [any()])
                   -> {Results :: [{node(), any()}], Down :: [node()]}.
multicall_ann(Nodes, Mod, Fun, Args) ->
    multicall_ann(Nodes, Mod, Fun, Args, infinity).

%% @doc Similar to {@link rpc:multicall/6}. Performs an RPC call to a list
%%      of nodes in parallel, returning a list of results as well as a list
%%      of nodes that are down/unreachable. The results will be returned in
%%      the same order as the input list, and each result is tagged with the
%%      corresponding node name.
-spec multicall_ann([node()], module(), function(), [any()], timeout())
                   -> {Results :: [{node(), any()}], Down :: [node()]}.
multicall_ann(Nodes, Mod, Fun, Args, Timeout) ->
    L = multi_rpc_ann(Nodes, Mod, Fun, Args, Timeout),
    {Results, DownAnn} =
        lists:partition(fun({_, Result}) ->
                                Result /= {badrpc, nodedown}
                        end, L),
    {Down, _} = lists:unzip(DownAnn),
    {Results, Down}.

%% @doc Convert a list of elements into an N-ary tree. This conversion
%%      works by treating the list as an array-based tree where, for
%%      example in a binary 2-ary tree, a node at index i has children
%%      2i and 2i+1. The conversion also supports a "cycles" mode where
%%      the array is logically wrapped around to ensure leaf nodes also
%%      have children by giving them backedges to other elements.
-spec build_tree(N :: integer(), Nodes :: [term()], Opts :: [term()])
                -> orddict:orddict(Node :: term(), Children :: [term()]).
build_tree(N, Nodes, Opts) ->
    case lists:member(cycles, Opts) of
        true -> 
            Expand = lists:flatten(lists:duplicate(N+1, Nodes));
        false ->
            Expand = Nodes
    end,
    {Tree, _} =
        lists:foldl(fun(Elm, {Result, Worklist}) ->
                            Len = erlang:min(N, length(Worklist)),
                            {Children, Rest} = lists:split(Len, Worklist),
                            NewResult = [{Elm, Children} | Result],
                            {NewResult, Rest}
                    end, {[], tl(Expand)}, Nodes),
    orddict:from_list(Tree).

orddict_delta(A, B) ->
    %% Pad both A and B to the same length
    DummyA = [{Key, '$none'} || {Key, _} <- B],
    A2 = orddict:merge(fun(_, Value, _) ->
                               Value
                       end, A, DummyA),

    DummyB = [{Key, '$none'} || {Key, _} <- A],
    B2 = orddict:merge(fun(_, Value, _) ->
                               Value
                       end, B, DummyB),

    %% Merge and filter out equal values
    Merged = orddict:merge(fun(_, AVal, BVal) ->
                                   {AVal, BVal}
                           end, A2, B2),
    Diff = orddict:filter(fun(_, {Same, Same}) ->
                                  false;
                             (_, _) ->
                                  true
                          end, Merged),
    Diff.

shuffle(L) ->
    N = 134217727, %% Largest small integer on 32-bit Erlang
    L2 = [{random:uniform(N), E} || E <- L],
    L3 = [E || {_, E} <- lists:sort(L2)],
    L3.

%% Returns a forced-lowercase architecture for this node
-spec get_arch () -> string().
get_arch () -> string:to_lower(erlang:system_info(system_architecture)).

%% Checks if this node is of a given architecture
-spec is_arch (atom()) -> boolean().
is_arch (linux) -> string:str(get_arch(),"linux") > 0;
is_arch (darwin) -> string:str(get_arch(),"darwin") > 0;
is_arch (sunos) -> string:str(get_arch(),"sunos") > 0;
is_arch (osx) -> is_arch(darwin);
is_arch (solaris) -> is_arch(sunos);
is_arch (Arch) -> throw({unsupported_architecture,Arch}).

format_ip_and_port(Ip, Port) when is_list(Ip) ->
    lists:flatten(io_lib:format("~s:~p",[Ip,Port]));
format_ip_and_port(Ip, Port) when is_tuple(Ip) ->
    lists:flatten(io_lib:format("~s:~p",[inet_parse:ntoa(Ip),
                                         Port])).
peername(Socket, Transport) ->
    case Transport:peername(Socket) of
        {ok, {Ip, Port}} ->
            format_ip_and_port(Ip, Port);
        {error, Reason} ->
            %% just return a string so JSON doesn't blow up
            lists:flatten(io_lib:format("error:~p", [Reason]))
    end.

sockname(Socket, Transport) ->
    case Transport:sockname(Socket) of
        {ok, {Ip, Port}} ->
            format_ip_and_port(Ip, Port);
        {error, Reason} ->
            %% just return a string so JSON doesn't blow up
            lists:flatten(io_lib:format("error:~p", [Reason]))
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

moment_test() ->
    M1 = riak_core_util:moment(),
    M2 = riak_core_util:moment(),
    ?assert(M2 >= M1).

clientid_uniqueness_test() ->
    ClientIds = [mkclientid('somenode@somehost') || _I <- lists:seq(0, 10000)],
    length(ClientIds) =:= length(sets:to_list(sets:from_list(ClientIds))).

build_tree_test() ->
    Flat = [1,
            11, 12,
            111, 112, 121, 122,
            1111, 1112, 1121, 1122, 1211, 1212, 1221, 1222],

    %% 2-ary tree decomposition
    ATree = [{1,    [  11,   12]},
             {11,   [ 111,  112]},
             {12,   [ 121,  122]},
             {111,  [1111, 1112]},
             {112,  [1121, 1122]},
             {121,  [1211, 1212]},
             {122,  [1221, 1222]},
             {1111, []},
             {1112, []},
             {1121, []},
             {1122, []},
             {1211, []},
             {1212, []},
             {1221, []},
             {1222, []}],

    %% 2-ary tree decomposition with cyclic wrap-around
    CTree = [{1,    [  11,   12]},
             {11,   [ 111,  112]},
             {12,   [ 121,  122]},
             {111,  [1111, 1112]},
             {112,  [1121, 1122]},
             {121,  [1211, 1212]},
             {122,  [1221, 1222]},
             {1111, [   1,   11]},
             {1112, [  12,  111]},
             {1121, [ 112,  121]},
             {1122, [ 122, 1111]},
             {1211, [1112, 1121]},
             {1212, [1122, 1211]},
             {1221, [1212, 1221]},
             {1222, [1222,    1]}],

    ?assertEqual(ATree, build_tree(2, Flat, [])),
    ?assertEqual(CTree, build_tree(2, Flat, [cycles])),
    ok.


counter_loop(N) ->
    receive
        {up, Pid} ->
            N2=N+1,
            Pid ! {counter_value, N2},
            counter_loop(N2);
        down ->
            counter_loop(N-1);
        exit ->
            exit(normal)
    end.

incr_counter(CounterPid) ->
    CounterPid ! {up, self()},
    receive
        {counter_value, N} -> N
    after
        3000 ->
            ?assert(false)
    end.

decr_counter(CounterPid) ->
    CounterPid ! down.

bounded_pmap_test_() ->
    Fun1 = fun(X) -> X+2 end,
    Tests =
    fun(CountPid) ->
        GFun = fun(Max) ->
                    fun(X) ->
                            ?assert(incr_counter(CountPid) =< Max),
                            timer:sleep(1),
                            decr_counter(CountPid),
                            Fun1(X)
                    end
               end,
        [
         fun() ->
             ?assertEqual(lists:seq(Fun1(1), Fun1(N)),
                          pmap(GFun(MaxP),
                               lists:seq(1, N), MaxP))
         end ||
         MaxP <- lists:seq(1,20),
         N <- lists:seq(0,10)
        ]
    end,
    {setup,
      fun() ->
          Pid = spawn_link(?MODULE, counter_loop, [0]),
          monitor(process, Pid),
          Pid
      end,
      fun(Pid) ->
          Pid ! exit,
          receive
              {'DOWN', _Ref, process, Pid, _Info} -> ok
          after
                  3000 ->
                  ?debugMsg("pmap counter process did not go down in time"),
                  ?assert(false)
          end,
          ok
      end,
      Tests
     }.

-endif.

