%% -------------------------------------------------------------------
%%
%% riak_test_util: utilities for test scripts
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc utilities for test scripts

-module(riak_core_test_util).

-ifdef(TEST).
-export([setup_mockring1/0,
         fake_ring/2,
         stop_pid/1,
         wait_for_pid/1,
         stop_pid/2,
         unlink_named_process/1]).
-include_lib("eunit/include/eunit.hrl").

stop_pid(undefined) ->
    ok;
stop_pid(Name) when is_atom(Name) ->
    stop_pid(whereis(Name));
stop_pid(Other) when not is_pid(Other) ->
    ok;
stop_pid(Pid) ->
    stop_pid(Pid, kill).

stop_pid(Other, _ExitType) when not is_pid(Other) ->
    ok;
stop_pid(Pid, ExitType) ->
    unlink(Pid),
    exit(Pid, ExitType),
    ok = wait_for_pid(Pid).

wait_for_pid(Pid) ->
    Mref = erlang:monitor(process, Pid),
    receive
        {'DOWN', Mref, process, _, _} ->
            ok
    after
        5000 ->
            {error, didnotexit}
    end.


unlink_named_process(Name) when is_atom(Name) ->
    unlink(whereis(Name)).

setup_mockring1() ->
    % requires a running riak_core_ring_manager, in test-mode is ok
    Ring0 = riak_core_ring:fresh(16,node()),
    Ring1 = riak_core_ring:add_member(node(), Ring0, 'othernode@otherhost'),
    Ring2 = riak_core_ring:add_member(node(), Ring1, 'othernode2@otherhost2'),

    Ring3 = lists:foldl(fun(_,R) ->
                               riak_core_ring:transfer_node(
                                 hd(riak_core_ring:my_indices(R)),
                                 'othernode@otherhost', R) end,
                        Ring2,[1,2,3,4,5,6]),
    Ring = lists:foldl(fun(_,R) ->
                               riak_core_ring:transfer_node(
                                 hd(riak_core_ring:my_indices(R)),
                                 'othernode2@otherhost2', R) end,
                       Ring3,[1,2,3,4,5,6]),
    riak_core_ring_manager:set_ring_global(Ring).

fake_ring(Size, NumNodes) ->
    ManyNodes = [list_to_atom("dev" ++ integer_to_list(X) ++ "@127.0.0.1")
                 || _ <- lists:seq(0, Size div NumNodes),
                    X <- lists:seq(1, NumNodes)],
    Nodes = lists:sublist(ManyNodes, Size),
    Inc = chash:ring_increment(Size),
    Indices = lists:seq(0, (Size-1)*Inc, Inc),
    Owners = lists:zip(Indices, Nodes),
    [Node|OtherNodes] = Nodes,
    Ring = riak_core_ring:fresh(Size, Node),
    Ring2 = lists:foldl(fun(OtherNode, RingAcc) ->
                                RingAcc2 = riak_core_ring:add_member(Node, RingAcc, OtherNode),
                                riak_core_ring:set_member(Node, RingAcc2, OtherNode,
                                                          valid, same_vclock)
                        end, Ring, OtherNodes),
    Ring3 = lists:foldl(fun({Idx, Owner}, RingAcc) ->
                                riak_core_ring:transfer_node(Idx, Owner, RingAcc)
                        end, Ring2, Owners),
    Ring3.

-endif. %TEST.
