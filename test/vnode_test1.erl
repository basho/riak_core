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
    MyPart = 42,                                % 42 is a nice partition #....
    PureOpts = make_general_pure_opts(FsmID, MyPart),
    InitIter = ?MUT:pure_start(FsmID, riak_bogus_vnode, MyPart, PureOpts),
    Events = [],

    {need_events, _, _} = X =
        ?PURE_DRIVER:run_to_completion(FsmID, ?MUT, InitIter, Events),
    [{res, X},
     {trace, ?PURE_DRIVER:get_trace(FsmID)}].

verify_bz520_test() ->
    FsmID = trivial_1st,
    MyPart = 42,                                % 42 is a nice partition #....
    OtherNode = other@node,
    OtherOpts = [{partition_owners, [{42, OtherNode}]}],
    BZ520_request_is_this = bz520_request_is_a_single_atom_not_tuple,
    PureOpts = [
                %% Setup should_handoff(): OtherNode is up
                {{riak_core_node_watcher, nodes},
                 fun([_App]) ->
                         [OtherNode]
                 end},
                %% Setup active(): backend is ready for handoff
                {{mod, handoff_starting},
                 fun([_, ModState]) ->
                         {true, ModState}
                 end},
                %% Setup start_handoff(): backend is not empty
                {{mod, is_empty},
                 fun([ModState]) ->
                         {false, ModState}
                 end},
                %% Setup start_handoff(): handoff lock is good
                {{riak_core_handoff_manager, get_handoff_lock},
                 fun(_) ->
                         {ok, {handoff_token, go}}
                 end},
                %% Setup start_handoff(): don't need to call
                %% riak_core_handoff_sender:start_link().

                %% TODO: All the steps above here are needed to get the
                %% existing riak_core_vnode code to set #state.handoff_node
                %% the way that we want it.  If we wanted to add
                %% debugging-only code, e.g. with an -ifdef(TEST) wrapper,
                %% then it could be worthwhile to simplify test setup.

                %% Setup vnode_handoff_command(): need to forward
                {{mod, handle_handoff_command},
                 fun([_Request, _Sender, ModState]) ->
                         {forward, ModState}
                 end},
                %% This is what we're after: put Req into the trace so that we
                %% can verify it.
                {{riak_core_vnode_master, command},
                 fun([_To2Tuple, Req, _Sender, _RegName]) ->
                         ?PURE_DRIVER:add_trace(FsmID, {verify_req, Req})
                 end}
               ] ++ make_general_pure_opts(FsmID, MyPart, OtherOpts),
    InitIter = ?MUT:pure_start(FsmID, riak_bogus_vnode, MyPart, PureOpts),
    Events = [
              {send_event, timeout},
              {send_event, ?MUT:make_vnode_request(BZ520_request_is_this)}
             ],

    {need_events, _, _} = X =
        ?PURE_DRIVER:run_to_completion(FsmID, ?MUT, InitIter, Events),
    Trace = ?PURE_DRIVER:get_trace(FsmID),
    CapturedRequest = proplists:get_value(verify_req, Trace),
    {got, BZ520_request_is_this} = {got, CapturedRequest},
    [{res, X},
     {trace, Trace}].

make_general_pure_opts(FsmID, PartitionInterval) ->
    make_general_pure_opts(FsmID, PartitionInterval, []).

make_general_pure_opts(FsmID, PartitionInterval = PI, Opts)
  when PartitionInterval > 0 ->
    NumParts = 8,
    SeedNode = r1@node,
    RingL0 = [{Idx, SeedNode} ||
                 Idx <- lists:seq(0, (NumParts*PI) - 1, PI)],
    RingL = case proplists:get_value(partition_owners, Opts) of
               undefined ->
                   RingL0;
               Others ->
                   lists:map(
                     fun({Part, _Node} = PN) ->
                             case proplists:get_value(Part, Others) of
                                 undefined ->
                                     PN;
                                 NewNode ->
                                     {Part, NewNode}
                             end
                     end, RingL0)
           end,
    Ring = {NumParts, RingL},
    ChState = {chstate, SeedNode, [], Ring, dict:new()}, % ugly hack
    NoReplyFun = fun(Args) -> {noreply, lists:last(Args)} end,
    [{debug, true},
     {{erlang, node}, SeedNode},
     {{riak_core_ring_manager, get_my_ring}, {ok, ChState}},
     {{riak_core_node_watcher, node_watcher_nodes}, [SeedNode]},
     {{app_helper, get_env},
      fun([riak_core, vnode_inactivity_timeout, _Default]) ->
              9999999999
      end},
     {{error_logger, error_msg},
      fun([Fmt, Args]) ->
              Str = io_lib:format(Fmt, Args),
              ?PURE_DRIVER:add_trace(FsmID, {error_logger, error_msg,
                                             lists:flatten(Str)})
      end},
     {{error_logger, info_msg},
      fun([Fmt, Args]) ->
              Str = io_lib:format(Fmt, Args),
              ?PURE_DRIVER:add_trace(FsmID, {error_logger, info_msg,
                                             lists:flatten(Str)})
      end},
     {{mod, reply},
      fun([Sender, Reply]) ->
              ?PURE_DRIVER:add_trace(FsmID, {reply, Sender, Reply})
      end},
     {{mod, init}, {ok, foo_mod_state}},
     {{mod, handle_command}, NoReplyFun},
     {{mod, handle_handoff_command}, NoReplyFun},
     {{mod, handle_handoff_data}, NoReplyFun},
     {{mod, handoff_starting}, NoReplyFun},
     {{mod, handoff_finished}, NoReplyFun},
     {{mod, handoff_cancelled}, NoReplyFun},
     {{mod, delete}, NoReplyFun},
     {{mod, is_empty}, NoReplyFun},
     {{mod, terminate}, NoReplyFun}
    ].
