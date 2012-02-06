%% -------------------------------------------------------------------
%%
%% riak_core_coverage_fsm: Distribute work to a covering set of VNodes.
%%
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

%% @doc The coverage fsm is a behavior used to create
%%      a plan to cover a set of VNodes, distribute
%%      a specified command to each VNode in the plan
%%      and then compile the results.
%%
%%      The number of VNodes required for full coverage
%%      is based on the number of partitions, the bucket
%%      n_val, and the number of primary VNodes from the
%%      preference list that are configured to be used by
%%      the module implementing this behavior.
%%
%%      Modules implementing this behavior should return
%%      a 5 member tuple from their init function that looks
%%      like this:
%%
%%         `{Request, VNodeSelector, NVal, PrimaryVNodeCoverage,
%%          NodeCheckService, VNodeMaster, Timeout, State}'
%%
%%      The description of the tuple members is as follows:
%%
%%      <ul>
%%      <li>Request - An opaque data structure that is used by
%%        the VNode to implement the specific coverage request.</li>
%%      <li>VNodeSelector - Either the atom `all' to indicate that
%%        enough VNodes must be available to achieve a minimal
%%        covering set or `allup' to use whatever VNodes are
%%        available even if they do not represent a fully covering
%%        set.</li>
%%      <li>NVal - Indicates the replication factor and is used to
%%        accurately create a minimal covering set of VNodes.</li>
%%      <li>PrimaryVNodeCoverage - The number of primary VNodes
%%      from the preference list to use in creating the coverage
%%      plan.</li>
%%      <li>NodeCheckService - The service to use to check for available
%%      nodes (e.g. riak_kv).</li>
%%      <li>VNodeMaster - The atom to use to reach the vnode master module.</li>
%%      <li>Timeout - The timeout interval for the coverage request.</li>
%%      <li>State - The initial state for the module.</li>
%%      </ul>
-module(riak_core_coverage_fsm).
-author('Kelly McLaughlin <kelly@basho.com>').

-include("riak_core_vnode.hrl").

-behaviour(gen_fsm).

%% API
-export([behaviour_info/1]).

-export([start_link/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
%% Test API
-export([test_link/5]).
-endif.

%% gen_fsm callbacks
-export([init/1,
         initialize/2,
         waiting_results/2,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-spec behaviour_info(atom()) -> 'undefined' | [{atom(), arity()}].
behaviour_info(callbacks) ->
    [
     {init, 2},
     {process_results, 2},
     {finish, 2}
    ];
behaviour_info(_) ->
    undefined.

-type req_id() :: non_neg_integer().
-type from() :: {atom(), req_id(), pid()}.

-record(state, {coverage_vnodes :: [{non_neg_integer(), node()}],
                mod :: atom(),
                mod_state :: tuple(),
                n_val :: pos_integer(),
                node_check_service :: module(),
                vnode_selector :: all | allup,
                pvc :: all | pos_integer(), % primary vnode coverage
                request :: tuple(),
                req_id :: req_id(),
                required_responses :: pos_integer(),
                response_count=0 :: non_neg_integer(),
                timeout :: timeout(),
                vnode_master :: atom()
               }).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Start a riak_core_coverage_fsm.
-spec start_link(module(), from(), [term()]) ->
                        {ok, pid()} | ignore | {error, term()}.
start_link(Mod, From, RequestArgs) ->
    gen_fsm:start_link(?MODULE, [Mod, From, RequestArgs], []).

%% ===================================================================
%% Test API
%% ===================================================================

-ifdef(TEST).

%% Create a coverage FSM for testing.
test_link(Mod, From, RequestArgs, _Options, StateProps) ->
    Timeout = 60000,
    gen_fsm:start_link(?MODULE,
                       {test,
                        [Mod,
                         From,
                         RequestArgs,
                         Timeout],
                        StateProps},
                       []).

-endif.

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

%% @private
init([Mod,
      From={_, ReqId, _},
      RequestArgs]) ->
    {Request, VNodeSelector, NVal, PrimaryVNodeCoverage,
     NodeCheckService, VNodeMaster, Timeout, ModState} =
        Mod:init(From, RequestArgs),
    StateData = #state{mod=Mod,
                       mod_state=ModState,
                       node_check_service=NodeCheckService,
                       vnode_selector=VNodeSelector,
                       n_val=NVal,
                       pvc = PrimaryVNodeCoverage,
                       request=Request,
                       req_id=ReqId,
                       timeout=Timeout,
                       vnode_master=VNodeMaster},
    {ok, initialize, StateData, 0};
init({test, Args, StateProps}) ->
    %% Call normal init
    {ok, initialize, StateData, 0} = init(Args),

    %% Then tweak the state record with entries provided by StateProps
    Fields = record_info(fields, state),
    FieldPos = lists:zip(Fields, lists:seq(2, length(Fields)+1)),
    F = fun({Field, Value}, State0) ->
                Pos = proplists:get_value(Field, FieldPos),
                setelement(Pos, State0, Value)
        end,
    TestStateData = lists:foldl(F, StateData, StateProps),

    %% Enter into the execute state, skipping any code that relies on the
    %% state of the rest of the system
    {ok, waiting_results, TestStateData, 0}.

%% @private
initialize(timeout, StateData0=#state{mod=Mod,
                                      mod_state=ModState,
                                      n_val=NVal,
                                      node_check_service=NodeCheckService,
                                      vnode_selector=VNodeSelector,
                                      pvc=PVC,
                                      request=Request,
                                      req_id=ReqId,
                                      timeout=Timeout,
                                      vnode_master=VNodeMaster}) ->
    CoveragePlan = riak_core_coverage_plan:create_plan(VNodeSelector,
                                                       NVal,
                                                       PVC,
                                                       ReqId,
                                                       NodeCheckService),
    case CoveragePlan of
        {error, Reason} ->
            Mod:finish({error, Reason}, ModState);
        {CoverageVNodes, FilterVNodes} ->
            Sender = {fsm, ReqId, self()},
            riak_core_vnode_master:coverage(Request,
                                            CoverageVNodes,
                                            FilterVNodes,
                                            Sender,
                                            VNodeMaster),
            StateData = StateData0#state{coverage_vnodes=CoverageVNodes},
            {next_state, waiting_results, StateData, Timeout}
    end.

%% @private
waiting_results({{ReqId, VNode}, Results},
                StateData=#state{coverage_vnodes=CoverageVNodes,
                                 mod=Mod,
                                 mod_state=ModState,
                                 req_id=ReqId,
                                 timeout=Timeout}) ->
    case Mod:process_results(Results, ModState) of
        {ok, UpdModState} ->
            UpdStateData = StateData#state{mod_state=UpdModState},
            {next_state, waiting_results, UpdStateData, Timeout};
        {done, UpdModState} ->
            UpdatedVNodes = lists:delete(VNode, CoverageVNodes),
            case UpdatedVNodes of
                [] ->
                    Mod:finish(clean, UpdModState);
                _ ->
                    UpdStateData =
                        StateData#state{coverage_vnodes=UpdatedVNodes,
                                        mod_state=UpdModState},
                    {next_state, waiting_results, UpdStateData, Timeout}
            end;
        Error ->
            Mod:finish(Error, ModState),
            {stop, Error, StateData}
    end;
waiting_results(timeout, #state{mod=Mod, mod_state=ModState}) ->
    Mod:finish({error, timeout}, ModState).

%% @private
handle_event(_Event, _StateName, State) ->
    {stop, badmsg, State}.

%% @private
handle_sync_event(_Event, _From, StateName, State) ->
    {next_state, StateName, State}.

%% @private
handle_info({'EXIT', _Pid, Reason}, _StateName, #state{mod=Mod,
                                                       mod_state=ModState}) ->
    Mod:finish({error, {node_failure, Reason}}, ModState);
handle_info({_ReqId, {ok, _Pid}},
            StateName,
            StateData=#state{timeout=Timeout}) ->
    %% Received a message from a coverage node that
    %% did not start up within the timeout. Just ignore
    %% the message and move on.
    {next_state, StateName, StateData, Timeout};
handle_info(_Info, _StateName, StateData) ->
    {stop, badmsg, StateData}.

%% @private
terminate(Reason, _StateName, _State) ->
    Reason.

%% @private
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.
