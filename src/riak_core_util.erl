%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2016 Basho Technologies, Inc.
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
         chash_key/1, chash_key/2,
         chash_std_keyfun/1,
         chash_bucketonly_keyfun/1,
         mkclientid/1,
         start_app_deps/1,
         build_tree/3,
         orddict_delta/2,
         safe_rpc/4,
         safe_rpc/5,
         rpc_every_member/4,
         rpc_every_member_ann/4,
         count/2,
         keydelete/2,
         multi_keydelete/2,
         multi_keydelete/3,
         compose/1,
         compose/2,
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
         sockname/2,
         sha/1,
         md5/1,
         make_fold_req/1,
         make_fold_req/2,
         make_fold_req/4,
         make_newest_fold_req/1,
         proxy_spawn/1,
         proxy/2,
         enable_job_class/1,
         enable_job_class/2,
         disable_job_class/1,
         disable_job_class/2,
         job_class_enabled/1,
         job_class_enabled/2,
         job_class_disabled_message/2,
         report_job_request_disposition/6,
         responsible_preflists/1,
         responsible_preflists/2,
         get_index_n/1,
         preflist_siblings/1
        ]).

-include("riak_core_vnode.hrl").

-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif. %% EQC
-include_lib("eunit/include/eunit.hrl").
-export([counter_loop/1,incr_counter/1,decr_counter/1]).
-endif. %% TEST

-type riak_core_ring() :: riak_core_ring:riak_core_ring().
-type index() :: non_neg_integer().
-type index_n() :: {index(), pos_integer()}.

%% R14 Compatibility
-compile({no_auto_import,[integer_to_list/2]}).

%% ===================================================================
%% Public API
%% ===================================================================

%% 719528 days from Jan 1, 0 to Jan 1, 1970
%%  *86400 seconds/day
-define(SEC_TO_EPOCH, 62167219200).

%% @spec moment() -> integer()
%% @doc Get the current "moment".  Current implementation is the
%%      number of seconds from year 0 to now, universal time, in
%%      the gregorian calendar.

moment() ->
    {Mega, Sec, _Micro} = os:timestamp(),
    (Mega * 1000000) + Sec + ?SEC_TO_EPOCH.

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
                          [erlang:phash2({riak_core_rand:uniform(),self()})]),
    TempDir = filename:join("/tmp", TmpId),
    case filelib:is_dir(TempDir) of
        true -> make_tmp_dir();
        false ->
            ok = file:make_dir(TempDir),
            TempDir
    end.

%% @doc Atomically/safely (to some reasonable level of durablity)
%% replace file `FN' with `Data'. NOTE: since 2.0.3 semantic changed
%% slightly: If `FN' cannot be opened, will not error with a
%% `badmatch', as before, but will instead return `{error, Reason}'
-spec replace_file(string(), iodata()) -> ok | {error, term()}.
replace_file(FN, Data) ->
    TmpFN = FN ++ ".tmp",
    case file:open(TmpFN, [write, raw]) of
        {ok, FH} ->
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
            end;
        Err ->
            Err
    end.

%% @doc Similar to {@link file:read_file/1} but uses raw file `I/O'
read_file(FName) ->
    {ok, FD} = file:open(FName, [read, raw, binary]),
    IOList = read_file(FD, []),
    ok = file:close(FD),
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

-ifdef(new_hash).
sha(Bin) ->
    crypto:hash(sha, Bin).

md5(Bin) ->
    crypto:hash(md5, Bin).
-else.
sha(Bin) ->
    crypto:sha(Bin).

md5(Bin) ->
    crypto:md5(Bin).
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
%%                            2  {error, term()}
%% @doc Ask each member node of the riak ring to reload the given
%%      Module.  Return is a list of the results of code:purge/1
%%      and code:load_file/1 on each node.
reload_all(Module) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    [{safe_rpc(Node, code, purge, [Module]),
     safe_rpc(Node, code, load_file, [Module])} ||
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
chash_key({Bucket,_Key}=BKey) ->
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    chash_key(BKey, BucketProps).

%% @spec chash_key(BKey :: riak_object:bkey(), [{atom(), any()}]) ->
%%          chash:index()
%% @doc Create a binary used for determining replica placement.
chash_key({Bucket,Key}, BucketProps) ->
    {_, {M, F}} = lists:keyfind(chash_keyfun, 1, BucketProps),
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
    _ = [ensure_started(A) || A <- DepApps],
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

%% @doc Applies `Pred' to each element in `List', and returns a count of how many
%% applications returned `true'.
-spec count(fun((term()) -> boolean()), [term()]) -> non_neg_integer().
count(Pred, List) ->
    FoldFun = fun(E, A) ->
                      case Pred(E) of
                          false -> A;
                          true -> A + 1
                      end
              end,
    lists:foldl(FoldFun, 0, List).

%% @doc Returns a copy of `TupleList' where the first occurrence of a tuple whose
%% first element compares equal to `Key' is deleted, if there is such a tuple.
%% Equivalent to `lists:keydelete(Key, 1, TupleList)'.
-spec keydelete(atom(), [tuple()]) -> [tuple()].
keydelete(Key, TupleList) ->
    lists:keydelete(Key, 1, TupleList).

%% @doc Returns a copy of `TupleList' where the first occurrence of a tuple whose
%% first element compares equal to any key in `KeysToDelete' is deleted, if
%% there is such a tuple.
-spec multi_keydelete([atom()], [tuple()]) -> [tuple()].
multi_keydelete(KeysToDelete, TupleList) ->
    multi_keydelete(KeysToDelete, 1, TupleList).

%% @doc Returns a copy of `TupleList' where the Nth occurrence of a tuple whose
%% first element compares equal to any key in `KeysToDelete' is deleted, if
%% there is such a tuple.
-spec multi_keydelete([atom()], non_neg_integer(), [tuple()]) -> [tuple()].
multi_keydelete(KeysToDelete, N, TupleList) ->
    lists:foldl(
      fun(Key, Acc) -> lists:keydelete(Key, N, Acc) end,
      TupleList,
      KeysToDelete).

%% @doc Function composition: returns a function that is the composition of
%% `F' and `G'.
-spec compose(F :: fun((B) -> C), G :: fun((A) -> B)) -> fun((A) -> C).
compose(F, G) when is_function(F, 1), is_function(G, 1) ->
    fun(X) ->
        F(G(X))
    end.

%% @doc Function composition: returns a function that is the composition of all
%% functions in the `Funs' list. Note that functions are composed from right to
%% left, so the final function in the `Funs' will be the first one invoked when
%% invoking the composed function.
-spec compose([fun((any()) -> any())]) -> fun((any()) -> any()).
compose([Fun]) ->
    Fun;
compose(Funs) when is_list(Funs) ->
    [Fun|Rest] = lists:reverse(Funs),
    lists:foldl(fun compose/2, Fun, Rest).

%% @doc Invoke function `F' over each element of list `L' in parallel,
%%      returning the results in the same order as the input list.
-spec pmap(F, L1) -> L2 when
      F :: fun((A) -> B),
      L1 :: [A],
      L2 :: [B].
pmap(F, L) ->
    Parent = self(),
    lists:foldl(
      fun(X, N) ->
              spawn_link(fun() ->
                                 Parent ! {pmap, N, F(X)}
                         end),
              N+1
      end, 0, L),
    L2 = [receive {pmap, N, R} -> {N,R} end || _ <- L],
    L3 = lists:keysort(1, L2),
    [R || {_,R} <- L3].

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


%% @doc Wraps an rpc:call/4 in a try/catch to handle the case where the
%%      'rex' process is not running on the remote node. This is safe in
%%      the sense that it won't crash the calling process if the rex
%%      process is down.
-spec safe_rpc(Node :: node(), Module :: atom(), Function :: atom(),
        Args :: [any()]) -> {'badrpc', any()} | any().
safe_rpc(Node, Module, Function, Args) ->
    try rpc:call(Node, Module, Function, Args) of
        Result ->
            Result
    catch
        exit:{noproc, _NoProcDetails} ->
            {badrpc, rpc_process_down}
    end.

%% @doc Wraps an rpc:call/5 in a try/catch to handle the case where the
%%      'rex' process is not running on the remote node. This is safe in
%%      the sense that it won't crash the calling process if the rex
%%      process is down.
-spec safe_rpc(Node :: node(), Module :: atom(), Function :: atom(),
        Args :: [any()], Timeout :: timeout()) -> {'badrpc', any()} | any().
safe_rpc(Node, Module, Function, Args, Timeout) ->
    try rpc:call(Node, Module, Function, Args, Timeout) of
        Result ->
            Result
    catch
        'EXIT':{noproc, _NoProcDetails} ->
            {badrpc, rpc_process_down}
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
rpc_every_member_ann(Module, Function, Args, Timeout) ->
    {ok, MyRing} = riak_core_ring_manager:get_my_ring(),
    Nodes = riak_core_ring:all_members(MyRing),
    {Results, Down} = multicall_ann(Nodes, Module, Function, Args, Timeout),
    {Results, Down}.

%% @doc Perform an RPC call to a list of nodes in parallel, returning the
%%      results in the same order as the input list.
-spec multi_rpc([node()], module(), atom(), [any()]) -> [any()].
multi_rpc(Nodes, Mod, Fun, Args) ->
    multi_rpc(Nodes, Mod, Fun, Args, infinity).

%% @doc Perform an RPC call to a list of nodes in parallel, returning the
%%      results in the same order as the input list.
-spec multi_rpc([node()], module(), atom(), [any()], timeout()) -> [any()].
multi_rpc(Nodes, Mod, Fun, Args, Timeout) ->
    pmap(fun(Node) ->
                 safe_rpc(Node, Mod, Fun, Args, Timeout)
         end, Nodes).

%% @doc Perform an RPC call to a list of nodes in parallel, returning the
%%      results in the same order as the input list. Each result is tagged
%%      with the corresponding node name.
-spec multi_rpc_ann([node()], module(), atom(), [any()])
                   -> [{node(), any()}].
multi_rpc_ann(Nodes, Mod, Fun, Args) ->
    multi_rpc_ann(Nodes, Mod, Fun, Args, infinity).

%% @doc Perform an RPC call to a list of nodes in parallel, returning the
%%      results in the same order as the input list. Each result is tagged
%%      with the corresponding node name.
-spec multi_rpc_ann([node()], module(), atom(), [any()], timeout())
                   -> [{node(), any()}].
multi_rpc_ann(Nodes, Mod, Fun, Args, Timeout) ->
    Results = multi_rpc(Nodes, Mod, Fun, Args, Timeout),
    lists:zip(Nodes, Results).

%% @doc Similar to {@link rpc:multicall/4}. Performs an RPC call to a list
%%      of nodes in parallel, returning a list of results as well as a list
%%      of nodes that are down/unreachable. The results will be returned in
%%      the same order as the input list, and each result is tagged with the
%%      corresponding node name.
-spec multicall_ann([node()], module(), atom(), [any()])
                   -> {Results :: [{node(), any()}], Down :: [node()]}.
multicall_ann(Nodes, Mod, Fun, Args) ->
    multicall_ann(Nodes, Mod, Fun, Args, infinity).

%% @doc Similar to {@link rpc:multicall/6}. Performs an RPC call to a list
%%      of nodes in parallel, returning a list of results as well as a list
%%      of nodes that are down/unreachable. The results will be returned in
%%      the same order as the input list, and each result is tagged with the
%%      corresponding node name.
-spec multicall_ann([node()], module(), atom(), [any()], timeout())
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
                -> orddict:orddict().
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
    L2 = [{riak_core_rand:uniform(N), E} || E <- L],
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

%% @doc Convert a #riak_core_fold_req_v? record to the cluster's maximum
%%      supported record version.

make_fold_req(#riak_core_fold_req_v1{foldfun=FoldFun, acc0=Acc0}) ->
    make_fold_req(FoldFun, Acc0, false, []);
make_fold_req(?FOLD_REQ{foldfun=FoldFun, acc0=Acc0,
                       forwardable=Forwardable, opts=Opts}) ->
    make_fold_req(FoldFun, Acc0, Forwardable, Opts).

make_fold_req(FoldFun, Acc0) ->
    make_fold_req(FoldFun, Acc0, false, []).

make_fold_req(FoldFun, Acc0, Forwardable, Opts) ->
    make_fold_reqv(riak_core_capability:get({riak_core, fold_req_version}, v1),
                   FoldFun, Acc0, Forwardable, Opts).

%% @doc Force a #riak_core_fold_req_v? record to the latest version,
%%      regardless of cluster support

make_newest_fold_req(#riak_core_fold_req_v1{foldfun=FoldFun, acc0=Acc0}) ->
    make_fold_reqv(v2, FoldFun, Acc0, false, []);
make_newest_fold_req(?FOLD_REQ{} = F) ->
    F.

%% @doc Spawn an intermediate proxy process to handle errors during gen_xxx
%%      calls.
proxy_spawn(Fun) ->
    %% Note: using spawn_monitor does not trigger selective receive
    %%       optimization, but spawn + monitor does. Silly Erlang.
    Pid = spawn(?MODULE, proxy, [self(), Fun]),
    MRef = monitor(process, Pid),
    Pid ! {proxy, MRef},
    receive
        {proxy_reply, MRef, Result} ->
            demonitor(MRef, [flush]),
            Result;
        {'DOWN', MRef, _, _, Reason} ->
            {error, Reason}
    end.


%% @private
make_fold_reqv(v1, FoldFun, Acc0, _Forwardable, _Opts)
  when is_function(FoldFun, 3) ->
    #riak_core_fold_req_v1{foldfun=FoldFun, acc0=Acc0};
make_fold_reqv(v2, FoldFun, Acc0, Forwardable, Opts)
  when is_function(FoldFun, 3)
       andalso (Forwardable == true orelse Forwardable == false)
       andalso is_list(Opts) ->
    ?FOLD_REQ{foldfun=FoldFun, acc0=Acc0,
              forwardable=Forwardable, opts=Opts}.

%% @private - used with proxy_spawn
proxy(Parent, Fun) ->
    _ = monitor(process, Parent),
    receive
        {proxy, MRef} ->
            Result = Fun(),
            Parent ! {proxy_reply, MRef, Result};
        {'DOWN', _, _, _, _} ->
            ok
    end.

-spec enable_job_class(atom(), atom()) -> ok | {error, term()}.
%% @doc Enables the specified Application/Operation job class.
%% This is the public API for use via RPC.
%% WARNING: This function is not suitable for parallel execution with itself
%% or its complement disable_job_class/2.
enable_job_class(Application, Operation)
        when erlang:is_atom(Application) andalso erlang:is_atom(Operation) ->
    enable_job_class({Application, Operation});
enable_job_class(Application, Operation) ->
    {error, {badarg, {Application, Operation}}}.

-spec disable_job_class(atom(), atom()) -> ok | {error, term()}.
%% @doc Disables the specified Application/Operation job class.
%% This is the public API for use via RPC.
%% WARNING: This function is not suitable for parallel execution with itself
%% or its complement enable_job_class/2.
disable_job_class(Application, Operation)
        when erlang:is_atom(Application) andalso erlang:is_atom(Operation) ->
    disable_job_class({Application, Operation});
disable_job_class(Application, Operation) ->
    {error, {badarg, {Application, Operation}}}.

-spec job_class_enabled(atom(), atom()) -> boolean() | {error, term()}.
%% @doc Reports whether the specified Application/Operation job class is enabled.
%% This is the public API for use via RPC.
job_class_enabled(Application, Operation)
        when erlang:is_atom(Application) andalso erlang:is_atom(Operation) ->
    job_class_enabled({Application, Operation});
job_class_enabled(Application, Operation) ->
    {error, {badarg, {Application, Operation}}}.

-spec enable_job_class(Class :: term()) -> ok | {error, term()}.
%% @doc Internal API to enable the specified job class.
%% WARNING:
%% * This function may not remain in this form once the Jobs API is live!
%% * Parameter types ARE NOT validated by the same rules as the public API!
%% You are STRONGLY advised to use enable_job_class/2.
enable_job_class(Class) ->
    case app_helper:get_env(riak_core, job_accept_class) of
        [_|_] = EnabledClasses ->
            case lists:member(Class, EnabledClasses) of
                true ->
                    ok;
                _ ->
                    application:set_env(
                        riak_core, job_accept_class, [Class | EnabledClasses])
            end;
        _ ->
            application:set_env(riak_core, job_accept_class, [Class])
    end.

-spec disable_job_class(Class :: term()) -> ok | {error, term()}.
%% @doc Internal API to disable the specified job class.
%% WARNING:
%% * This function may not remain in this form once the Jobs API is live!
%% * Parameter types ARE NOT validated by the same rules as the public API!
%% You are STRONGLY advised to use disable_job_class/2.
disable_job_class(Class) ->
    case app_helper:get_env(riak_core, job_accept_class) of
        [_|_] = EnabledClasses ->
            case lists:member(Class, EnabledClasses) of
                false ->
                    ok;
                _ ->
                    application:set_env(riak_core, job_accept_class,
                        lists:delete(Class, EnabledClasses))
            end;
        _ ->
            ok
    end.

-spec job_class_enabled(Class :: term()) -> boolean().
%% @doc Internal API to determine whether to accept/reject a job.
%% WARNING:
%% * This function may not remain in this form once the Jobs API is live!
%% * Parameter types ARE NOT validated by the same rules as the public API!
%% You are STRONGLY advised to use job_class_enabled/2.
job_class_enabled(Class) ->
    case app_helper:get_env(riak_core, job_accept_class) of
        undefined ->
            true;
        [] ->
            false;
        [_|_] = EnabledClasses ->
            lists:member(Class, EnabledClasses);
        Other ->
            % Don't crash if it's not a list - that should never be the case,
            % but since the value *can* be manipulated externally be more
            % accommodating. If someone mucks it up, nothing's going to be
            % allowed, but give them a chance to catch on instead of crashing.
            _ = lager:error(
                "riak_core.job_accept_class is not a list: ~p", [Other]),
            false
    end.

-spec job_class_disabled_message(ReturnType :: atom(), Class :: term())
        -> binary() | string().
%% @doc The error message to be returned to a client for a disabled job class.
%% WARNING:
%% * This function is likely to be extended to accept a Job as well as a Class
%%   when the Jobs API is live.
job_class_disabled_message(binary, Class) ->
    erlang:list_to_binary(job_class_disabled_message(text, Class));
job_class_disabled_message(text, Class) ->
    lists:flatten(io_lib:format("Operation '~p' is not enabled", [Class])).

-spec report_job_request_disposition(Accepted :: boolean(), Class :: term(),
    Mod :: module(), Func :: atom(), Line :: pos_integer(), Client :: term())
        -> ok | {error, term()}.
%% @doc Report/record the disposition of an async job request.
%%
%% Logs an appropriate message and reports to whoever needs to know.
%% WARNING:
%% * This function is likely to be extended to accept a Job as well as a Class
%%   when the Jobs API is live.
%%
%% Parameters:
%%  * Accepted - Whether the specified job Class is enabled.
%%  * Class - The Class of the job, by convention {Application, Operation}.
%%  * Mod/Func/Line - The Module, function, and source line number,
%%    respectively, that will be reported as the source of the call.
%%  * Client - Any term indicating the originator of the request.
%%    By convention, when meaningful client identification information is not
%%    available, Client is an atom representing the protocol through which the
%%    request was received.
%%
report_job_request_disposition(true, Class, Mod, Func, Line, Client) ->
    lager:log(debug,
        [{pid, erlang:self()}, {module, Mod}, {function, Func}, {line, Line}],
        "Request '~p' accepted from ~p", [Class, Client]);
report_job_request_disposition(false, Class, Mod, Func, Line, Client) ->
    lager:log(warning,
        [{pid, erlang:self()}, {module, Mod}, {function, Func}, {line, Line}],
        "Request '~p' disabled from ~p", [Class, Client]).

%% ===================================================================
%% Preflist utility functions
%% ===================================================================

%% @doc Given a bucket/key, determine the associated preflist index_n.
-spec get_index_n({binary(), binary()}) -> index_n().
get_index_n({Bucket, Key}) ->
    BucketProps = riak_core_bucket:get_bucket(Bucket),
    N = proplists:get_value(n_val, BucketProps),
    ChashKey = riak_core_util:chash_key({Bucket, Key}),
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    Index = chashbin:responsible_index(ChashKey, CHBin),
    {Index, N}.

%% @doc Given an index, determine all sibling indices that participate in one
%%      or more preflists with the specified index.
-spec preflist_siblings(index()) -> [index()].
preflist_siblings(Index) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    preflist_siblings(Index, Ring).

%% @doc See {@link preflist_siblings/1}.
-spec preflist_siblings(index(), riak_core_ring()) -> [index()].
preflist_siblings(Index, Ring) ->
    MaxN = determine_max_n(Ring),
    preflist_siblings(Index, MaxN, Ring).

-spec preflist_siblings(index(), pos_integer(), riak_core_ring()) -> [index()].
preflist_siblings(Index, N, Ring) ->
    IndexBin = <<Index:160/integer>>,
    PL = riak_core_ring:preflist(IndexBin, Ring),
    Indices = [Idx || {Idx, _} <- PL],
    RevIndices = lists:reverse(Indices),
    {Succ, _} = lists:split(N-1, Indices),
    {Pred, _} = lists:split(N-1, tl(RevIndices)),
    lists:reverse(Pred) ++ Succ.

-spec responsible_preflists(index()) -> [index_n()].
responsible_preflists(Index) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    responsible_preflists(Index, Ring).

-spec responsible_preflists(index(), riak_core_ring()) -> [index_n()].
responsible_preflists(Index, Ring) ->
    AllN = determine_all_n(Ring),
    responsible_preflists(Index, AllN, Ring).

-spec responsible_preflists(index(), [pos_integer(),...], riak_core_ring())
                           -> [index_n()].
responsible_preflists(Index, AllN, Ring) ->
    IndexBin = <<Index:160/integer>>,
    PL = riak_core_ring:preflist(IndexBin, Ring),
    Indices = [Idx || {Idx, _} <- PL],
    RevIndices = lists:reverse(Indices),
    lists:flatmap(fun(N) ->
                          responsible_preflists_n(RevIndices, N)
                  end, AllN).

-spec responsible_preflists_n([index()], pos_integer()) -> [index_n()].
responsible_preflists_n(RevIndices, N) ->
    {Pred, _} = lists:split(N, RevIndices),
    [{Idx, N} || Idx <- lists:reverse(Pred)].


-spec determine_max_n(riak_core_ring()) -> pos_integer().
determine_max_n(Ring) ->
    lists:max(determine_all_n(Ring)).

-spec determine_all_n(riak_core_ring()) -> [pos_integer(),...].
determine_all_n(Ring) ->
    Buckets = riak_core_ring:get_buckets(Ring),
    BucketProps = [riak_core_bucket:get_bucket(Bucket, Ring) || Bucket <- Buckets],
    Default = app_helper:get_env(riak_core, default_bucket_props),
    DefaultN = proplists:get_value(n_val, Default),
    AllN = lists:foldl(fun(Props, AllN) ->
                               N = proplists:get_value(n_val, Props),
                               ordsets:add_element(N, AllN)
                       end, [DefaultN], BucketProps),
    AllN.


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

multi_keydelete_test_() ->
    Languages = [{lisp, 1958},
                 {ml, 1973},
                 {erlang, 1986},
                 {haskell, 1990},
                 {ocaml, 1996},
                 {clojure, 2007},
                 {elixir, 2012}],
    ?_assertMatch(
       [{lisp, _}, {ml, _}, {erlang, _}, {haskell, _}],
       multi_keydelete([ocaml, clojure, elixir], Languages)).

compose_test_() ->
    Upper = fun string:to_upper/1,
    Reverse = fun lists:reverse/1,
    Strip = fun(S) -> string:strip(S, both, $!) end,
    StripReverseUpper = compose([Upper, Reverse, Strip]),

    Increment = fun(N) when is_integer(N) -> N + 1 end,
    Double = fun(N) when is_integer(N) -> N * 2 end,
    Square = fun(N) when is_integer(N) -> N * N end,
    SquareDoubleIncrement = compose([Increment, Double, Square]),

    CompatibleTypes = compose(Increment,
                              fun(X) when is_list(X) -> list_to_integer(X) end),
    IncompatibleTypes = compose(Increment,
                                fun(X) when is_binary(X) -> binary_to_list(X) end),
    [?_assertEqual("DLROW OLLEH", StripReverseUpper("Hello world!")),
     ?_assertEqual(Increment(Double(Square(3))), SquareDoubleIncrement(3)),
     ?_assertMatch(4, CompatibleTypes("3")),
     ?_assertError(function_clause, IncompatibleTypes(<<"42">>)),
     ?_assertError(function_clause, compose(fun(X, Y) -> {X, Y} end, fun(X) -> X end))].

pmap_test_() ->
    Fgood = fun(X) -> 2 * X end,
    Fbad = fun(3) -> throw(die_on_3);
              (X) -> Fgood(X)
           end,
    Lin = [1,2,3,4],
    Lout = [2,4,6,8],
    {setup,
     fun() -> error_logger:tty(false) end,
     fun(_) -> error_logger:tty(true) end,
     [fun() ->
              % Test simple map case
              ?assertEqual(Lout, pmap(Fgood, Lin)),
              % Verify a crashing process will not stall pmap
              Parent = self(),
              Pid = spawn(fun() ->
                                  % Caller trapping exits causes stall!!
                                  % TODO: Consider pmapping in a spawned proc
                                  % process_flag(trap_exit, true),
                                  pmap(Fbad, Lin),
                                  ?debugMsg("pmap finished just fine"),
                                  Parent ! no_crash_yo
                          end),
              MonRef = monitor(process, Pid),
              receive
                  {'DOWN', MonRef, _, _, _} ->
                      ok;
                  no_crash_yo ->
                      ?assert(pmap_did_not_crash_as_expected)
              end
      end
     ]}.

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

make_fold_req_test_() ->
    {setup,
     fun() ->
             meck:unload(),
             meck:new(riak_core_capability, [passthrough])
     end,
     fun(_) ->
             ok
     end,
     [
      fun() ->
              FoldFun = fun(_, _, _) -> ok end,
              Acc0 = acc0,
              Forw = true,
              Opts = [opts],
              F_1 = #riak_core_fold_req_v1{foldfun=FoldFun, acc0=Acc0},
              F_2 = #riak_core_fold_req_v2{foldfun=FoldFun, acc0=Acc0,
                                           forwardable=Forw, opts=Opts},
              F_2_default = #riak_core_fold_req_v2{foldfun=FoldFun, acc0=Acc0,
                                                   forwardable=false, opts=[]},
              Newest = fun() -> F_2_default = make_newest_fold_req(F_1),
                                F_2         = make_newest_fold_req(F_2),
                                ok
                       end,

              meck:expect(riak_core_capability, get,
                          fun({riak_core, fold_req_version}, _) -> v1 end),
              F_1         = make_fold_req(F_1),
              F_1         = make_fold_req(F_2),
              F_1         = make_fold_req(FoldFun, Acc0),
              F_1         = make_fold_req(FoldFun, Acc0, Forw, Opts),
              ok = Newest(),

              meck:expect(riak_core_capability, get,
                          fun({riak_core, fold_req_version}, _) -> v2 end),
              F_2_default = make_fold_req(F_1),
              F_2         = make_fold_req(F_2),
              F_2_default = make_fold_req(FoldFun, Acc0),
              F_2         = make_fold_req(FoldFun, Acc0, Forw, Opts),
              ok = Newest(),
              %% It seems you could unload `meck' in the test teardown,
              %% but that sometimes causes the eunit process to crash.
              %% Instead, unload at end of test.
              meck:unload()
      end
     ]
    }.

proxy_spawn_test() ->
    A = proxy_spawn(fun() -> a end),
    ?assertEqual(a, A),
    B = proxy_spawn(fun() -> exit(killer_fun) end),
    ?assertEqual({error, killer_fun}, B),

    %% Ensure no errant 'DOWN' messages
    receive
        {'DOWN', _, _, _, _}=Msg ->
            throw({error, {badmsg, Msg}});
        _ ->
            ok
    after 1000 ->
        ok
    end.

-ifdef(EQC).

count_test() ->
    ?assert(eqc:quickcheck(prop_count_correct())).

prop_count_correct() ->
    ?FORALL(List, list(bool()),
            count(fun(E) -> E end, List) =:= length([E || E <- List, E])).

-endif. %% EQC
-endif. %% TEST
