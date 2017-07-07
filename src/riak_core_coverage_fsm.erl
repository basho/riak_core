%% -------------------------------------------------------------------
%%
%% riak_core_coverage_fsm: Distribute work to a covering set of VNodes.
%%
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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
%%      a 9 member tuple from their init function that looks
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
%%      <li>VNodeSelector - If using the standard riak_core_coverage_plan
%%          module, 'all' for all VNodes or 'allup' for all reachable.
%%          If using an alternative coverage plan generator, whatever
%%          value is useful to its `create_plan' function; will be passed
%%          as the first argument.</li>
%%      <li>NVal - Indicates the replication factor and is used to
%%        accurately create a minimal covering set of VNodes.</li>
%%      <li>PrimaryVNodeCoverage - The number of primary VNodes
%%      from the preference list to use in creating the coverage
%%      plan. Typically just 1.</li>
%%      <li>NodeCheckService - The service to use to check for available
%%      nodes (e.g. riak_kv).</li>
%%      <li>VNodeMaster - The atom to use to reach the vnode master module.</li>
%%      <li>Timeout - The timeout interval for the coverage request.</li>
%%      <li>PlannerMod - The module which defines `create_plan' and optionally
%%          functions to support a coverage query API</li>
%%      <li>State - The initial state for the module.</li>
%%      </ul>
-module(riak_core_coverage_fsm).

-include("riak_core_vnode.hrl").

-behaviour(gen_fsm).

%% API
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

-type mod_state() :: term().

-define(DEFAULT_TIMEOUT, 60000*8).

-type req_id() :: non_neg_integer().
-type from() :: {atom(), req_id(), pid()}.
-type pvc() :: all | pos_integer().
-type request() :: tuple().
-type index() :: chash:index_as_int().

-record(state, {coverage_vnodes :: [{non_neg_integer(), node()}] | undefined,
                mod :: atom(),
                mod_state :: mod_state(),
                n_val :: pos_integer(),
                node_check_service :: module(),
                %% `vnode_selector' can be any useful value for different
                %% `riak_core' applications that define their own coverage
                %% plan module
                vnode_selector :: vnode_selector() | vnode_coverage() | term(),
                pvc :: all | pos_integer(), % primary vnode coverage
                request :: tuple(),
                req_id :: req_id(),
                required_responses=1 :: pos_integer(),
                response_count=0 :: non_neg_integer(),
                timeout :: timeout(),
                vnode_master :: atom(),
                plan_fun :: function(),
                process_fun :: function(),
                coverage_plan_fn :: function()
               }).

-callback init(From::from(), RequestArgs::any()) ->
    {
      Request :: request(),
      VNodeSelector:: vnode_selector(),
      NVal :: pos_integer(),
      PrimaryVNodeCoverage :: pvc(),
      NodeCheckService :: module(),
      VNodeMaster :: atom(),
      Timeout :: pos_integer(),
      CoveragePlanRet :: atom()
                       | fun((riak_core_coverage_plan:vnode_selector(),
                              pos_integer(),
                              pos_integer(),
                              riak_core_coverage_plan:req_id(),
                              atom(), term()) ->
                                    {error, term()} |
                                    riak_core_coverage_plan:coverage_plan()),
      ModState :: tuple()
    }.

-callback process_results(Results :: term(), ModState :: tuple()) ->
    {ok, NewModState :: tuple()} |
    {done, NewModState :: tuple()} |
    {error, Reason :: term()}.

-callback process_results(VNode :: pos_integer(),
                          Results::term(), ModState :: tuple()) ->
    {ok, NewModState :: tuple()} |
    {done, NewModState :: tuple()} |
    {error, Reason :: term()}.
-optional_callbacks([process_results/3]).

-callback finish({error, Reason::term()} | clean, ModState :: tuple()) ->
    {error, Reason::term()} |
    {'stop', Reason::term(), ModState :: tuple()}.

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
     NodeCheckService, VNodeMaster, Timeout, CoveragePlanRet, ModState} =
        Mod:init(From, RequestArgs),

    %% As part of fixing listkeys, we have changed the API model so
    %% that modules with riak_core_coverage_fsm behaviour now return a
    %% coverage plan function rather than a module.  The four
    %% core_coverage_fsm modules used regularly in RiakTS have all
    %% been changed to return functions.  
    %%
    %% However, to ensure backwards compatibility with any modules
    %% that may have been missed, we add a check here in case someone
    %% is still sending us back a module atom.  In that case, convert
    %% the coverage plan module to the appropriate function for use
    %% further down the stack

    CoveragePlanFn = 
        case is_atom(CoveragePlanRet) of
            true ->
                fun CoveragePlanRet:create_plan/6;
            _ ->
                CoveragePlanRet
        end,

    maybe_start_timeout_timer(Timeout),
    PlanFun = plan_callback(Mod),
    ProcessFun = process_results_callback(Mod),
    StateData = #state{mod=Mod,
                       mod_state=ModState,
                       node_check_service=NodeCheckService,
                       vnode_selector=VNodeSelector,
                       n_val=NVal,
                       pvc = PrimaryVNodeCoverage,
                       request=Request,
                       req_id=ReqId,
                       timeout=infinity,
                       vnode_master=VNodeMaster,
                       plan_fun = PlanFun,
                       process_fun = ProcessFun,
                       coverage_plan_fn = CoveragePlanFn},
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
maybe_start_timeout_timer(infinity) ->
    ok;
maybe_start_timeout_timer(Bad) when not is_integer(Bad) ->
    maybe_start_timeout_timer(?DEFAULT_TIMEOUT);
maybe_start_timeout_timer(Timeout) ->
    gen_fsm:start_timer(Timeout, {timer_expired, Timeout}),
    ok.

%% @private
find_plan(_CoveragePlanFn, #vnode_coverage{}=Plan, _NVal, _PVC, _ReqId, _Service,
          _Request) ->
    interpret_plan(Plan);
find_plan(CoveragePlanFn, Target, NVal, PVC, ReqId, Service, Request) ->
    CoveragePlanFn(Target, NVal, PVC, ReqId, Service, Request).

%% @private
%% Take a `vnode_coverage' record and interpret it as a mini coverage plan
-spec interpret_plan(vnode_coverage()) ->
                            {list({index(), node()}),
                             list({index(), list(index())|tuple()})}.
interpret_plan(#vnode_coverage{vnode_identifier  = TargetHash,
                               partition_filters = [],
                               subpartition = undefined}) ->
    {[{TargetHash, node()}], []};
interpret_plan(#vnode_coverage{vnode_identifier  = TargetHash,
                               partition_filters = HashFilters,
                               subpartition = undefined}) ->
    {[{TargetHash, node()}], [{TargetHash, HashFilters}]};
interpret_plan(#vnode_coverage{vnode_identifier = TargetHash,
                               subpartition     = {Mask, BSL}}) ->
    {[{TargetHash, node()}], [{TargetHash, {Mask, BSL}}]}.


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
                                      vnode_master=VNodeMaster,
                                      plan_fun = PlanFun,
                                      coverage_plan_fn = CoveragePlanFn}) ->
    CoveragePlan = find_plan(CoveragePlanFn,
                             VNodeSelector,
                             NVal,
                             PVC,
                             ReqId,
                             NodeCheckService,
                             Request),
    case CoveragePlan of
        {error, Reason} ->
            Mod:finish({error, Reason}, ModState);
        {CoverageVNodes, FilterVNodes} ->
            {ok, UpModState} = PlanFun(CoverageVNodes, ModState),
            Sender = {fsm, ReqId, self()},
            riak_core_vnode_master:coverage(Request,
                                            CoverageVNodes,
                                            FilterVNodes,
                                            Sender,
                                            VNodeMaster),
            StateData = StateData0#state{coverage_vnodes=CoverageVNodes, mod_state=UpModState},
            {next_state, waiting_results, StateData, Timeout}
    end.

%% @private
waiting_results({{ReqId, VNode}, Results},
                StateData=#state{coverage_vnodes=CoverageVNodes,
                                 mod=Mod,
                                 mod_state=ModState,
                                 req_id=ReqId,
                                 timeout=Timeout,
                                 process_fun = ProcessFun}) ->
    case ProcessFun(VNode, Results, ModState) of
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
            Mod:finish(Error, ModState)
    end;
waiting_results({timeout, _, _}, #state{mod=Mod, mod_state=ModState}) ->
    Mod:finish({error, timeout}, ModState);
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

%% This is to avoid expensive module_info calls, which were consuming
%% 30% of the query-path time for small queries
%%
%% Instead we try Mod:plan/2, if undef, we define it.  On success or
%% match error on atoms, we pass on Mod:plan/2 

plan_callback(Mod) ->
    SuccessFun = fun(CoverageVNodes, ModState) ->
                         Mod:plan(CoverageVNodes, ModState) end,
    try
        SuccessFun(a, b),
        SuccessFun
    catch
        error:undef ->
            fun(_, ModState) ->
                    {ok, ModState} end;
        _:_ -> %% If Mod:plan(a, b) fails on atoms
            SuccessFun
    end.

%% This is to avoid expensive module_info calls, which were consuming
%% 30% of the query-path time for small queries
%%
%% Instead we try Mod:process_results/3, if undef, we define it.  On success or
%% match error on atoms, we pass on Mod:process_results/3

process_results_callback(Mod) ->
    SuccessFun = fun(VNode, Results, ModState) ->
                         Mod:process_results(VNode, Results, ModState) end,
    try
        SuccessFun(a,b,c),
        SuccessFun
    catch
        error:undef ->
            fun(_VNode, Results, ModState) ->
                    Mod:process_results(Results, ModState) end;
        _:_ -> %% If Mod:plan(a, b, c) fails on atoms
            SuccessFun
    end.
