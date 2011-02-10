%%
%% Copyright (c) 2011 Basho Technologies, Inc.  All Rights Reserved.
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

-module(vnode_test1).

-compile(export_all).

-define(PURE_DRIVER, gen_fsm_test_driver).
-define(MUT, riak_core_vnode).

pure_1st_trivial_test() ->
    FsmID = trivial_1st,
    NumParts = 8,
    SeedNode = r1@node,
    MyPart = 42,
    Ring = {NumParts, [{Idx, SeedNode} ||
                          Idx <- lists:seq(0, NumParts*MyPart-1, MyPart)]},
    ChState = {chstate, SeedNode, [], Ring, dict:new()}, % ugly hack
    PureOpts = [{debug, true},
                {node, SeedNode},
                {get_my_ring, ChState},
                {node_watcher_nodes, [SeedNode]}
               ],
    
    InitIter = ?MUT:pure_start(FsmID, mock_vnode, MyPart, PureOpts),
    Events = [],
    {need_events, _, _} = X =
        ?PURE_DRIVER:run_to_completion(FsmID, ?MUT, InitIter, Events),
    [{res, X},
     {trace, ?PURE_DRIVER:get_trace(FsmID)}].
