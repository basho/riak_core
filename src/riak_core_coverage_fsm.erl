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
%%         `{ok, ModFun, PrimaryVNodeCoverage,
%%            NodeCheckService, VNodeMaster}'
%%
%%      The description of the tuple members is as follows:
%%
%%      <ul>
%%      <li>ModFun - The function that will be called by each
%%               VNode that is part of the coverage plan.</li>
%%      <li>PrimaryVNodeCoverage - The number of primary VNodes
%%      from the preference list to use in creating the coverage
%%      plan.</li>
%%      <li>NodeCheckService - The service to use to check for available
%%      nodes (e.g. riak_kv).</li>
%%      <li>VNodeMaster - The atom to use to reach the vnode master module.</li>
%%      </ul>
-module(riak_core_coverage_fsm).
-author('Kelly McLaughlin <kelly@basho.com>').

-include_lib("riak_core_vnode.hrl").

-behaviour(gen_fsm).

%% API
-export([behaviour_info/1]).

-export([start_link/6,
         start_link/7]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
%% Test API
-export([test_link/9, test_link/7]).
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
     {init, 0},
     {process_results, 3}
    ];
behaviour_info(_) ->
    undefined.

-type bucket() :: all | binary().
-type filter() :: none | fun() | [mfa()].
-type req_id() :: non_neg_integer().
-type modfun() :: {module(), fun()}.
-type from() :: {raw, req_id(), pid()}.

-record(state, {args :: [term()],
                bucket :: bucket(),
                client_type :: atom(),
                coverage_vnodes :: [{non_neg_integer(), node()}],
                filter :: filter(),
                from :: from(),
                mod :: atom(),
                modfun :: modfun(),
                node_check_service :: module(),
                pvc :: all | pos_integer(), % primary vnode coverage
                required_responses :: pos_integer(),
                response_count=0 :: non_neg_integer(),
                timeout :: timeout(),
                vnode_master :: atom()
               }).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Start a riak_core_coverage_fsm.
-spec start_link(module(), from(), filter(), list(), timeout(), atom()) ->
                        {ok, pid()} | ignore | {error, term()}.
start_link(Mod, From, ItemFilter, RequestArgs, Timeout, ClientType) ->
    start_link(Mod, From, all, ItemFilter, RequestArgs, Timeout, ClientType).

%% @doc Start a riak_core_coverage_fsm.
-spec start_link(module(), from(), bucket(), filter(), list(), timeout(), atom()) ->
                        {ok, pid()} | ignore | {error, term()}.
start_link(Mod, From, Bucket, ItemFilter, RequestArgs, Timeout, ClientType) ->
    gen_fsm:start_link(?MODULE,
                  [Mod, From, Bucket, ItemFilter, RequestArgs, Timeout, ClientType], []).

%% ===================================================================
%% Test API
%% ===================================================================

-ifdef(TEST).
%% Create a coverage FSM for testing. StateProps must include
%% starttime - start time in gregorian seconds
%% n - N-value for request (is grabbed from bucket props in prepare)
%% bucket_props - bucket properties
%% preflist2 - [{{Idx,Node},primary|fallback}] preference list
%%
test_link(Mod, ReqId, Bucket, ItemFilter, RequestArgs, R, Timeout, From, StateProps) ->
    test_link(Mod,
              {raw, ReqId, From},
              Bucket,
              ItemFilter,
              RequestArgs,
              [{r, R}, {timeout, Timeout}],
              StateProps).

test_link(Mod, From, Bucket, ItemFilter, RequestArgs, _Options, StateProps) ->
    Timeout = 60000,
    ClientType = plain,
    gen_fsm:start_link(?MODULE, 
                       {test, 
                        [Mod, 
                         From, 
                         Bucket,
                         ItemFilter,
                         RequestArgs,
                         Timeout,
                         ClientType],
                        StateProps},
                       []).

-endif.

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

%% @private
init([Mod,
      From={raw, ReqId, ClientPid},
      Bucket,
      ItemFilter,
      RequestArgs,
      Timeout,
      ClientType]) ->
    {ok, ModFun, PrimaryVnodeCoverage, NodeCheckService, VNodeMaster} =
        Mod:init(),
    case Bucket of
        all ->
            Args = [self(), ReqId] ++ RequestArgs;
        _ ->
            Args = [self(), ReqId, Bucket] ++ RequestArgs
    end,
    StateData = #state{args=Args,
                       bucket=Bucket,
                       client_type=ClientType,
                       filter=ItemFilter,
                       from=From,
                       mod=Mod,
                       modfun=ModFun,
                       node_check_service=NodeCheckService,
                       pvc = PrimaryVnodeCoverage,
                       timeout=Timeout,
                       vnode_master=VNodeMaster},
    case ClientType of
        %% Link to the mapred job so we die if the job dies
        mapred ->
            link(ClientPid);
        _ ->
            ok
    end,
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
initialize(timeout, StateData0=#state{args=Args,
                                      bucket=Bucket,
                                      filter=ItemFilter,
                                      from={_, ReqId, _},
                                      modfun=ModFun,
                                      node_check_service=NodeCheckService,
                                      pvc=PVC,
                                      timeout=Timeout,
                                      vnode_master=VNodeMaster}) ->
    Request = ?COVERAGE_REQ{args=Args,
                            bucket=Bucket,
                            filter=ItemFilter,
                            modfun=ModFun,
                            req_id=ReqId},
    CoveragePlan = riak_core_coverage_plan:create_plan(Bucket,
                                                       PVC,
                                                       ReqId,
                                                       NodeCheckService),
    case CoveragePlan of
        {error, Reason} ->
            finish({error, Reason}, StateData0);
        {_, CoverageVNodes, _} ->
            riak_core_vnode_master:coverage(Request,
                                            CoveragePlan,
                                            ItemFilter,
                                            VNodeMaster),
            StateData = StateData0#state{coverage_vnodes=CoverageVNodes},
            {next_state, waiting_results, StateData, Timeout}
    end.

%% @private
waiting_results({ReqId, {results, VNode, Results}},
           StateData=#state{client_type=ClientType,
                            coverage_vnodes=CoverageVNodes,
                            from=From={raw, ReqId, _},
                            mod=Mod,
                            timeout=Timeout}) ->
    case lists:member(VNode, CoverageVNodes) of
        true -> % Received an expected response from a Vnode
            Mod:process_results(Results, ClientType, From);
        false -> % Ignore a response from a VNode that
                 % is not part of the coverage plan
           ignore
    end,
    {next_state, waiting_results, StateData, Timeout};
waiting_results({ReqId, {final_results, VNode, Results}},
           StateData=#state{client_type=ClientType,
                            coverage_vnodes=CoverageVNodes,
                            from=From={raw, ReqId, _},
                            mod=Mod,
                            timeout=Timeout}) ->
    case lists:member(VNode, CoverageVNodes) of
        true -> % Received an expected response from a Vnode
            Mod:process_results(Results, ClientType, From),
            UpdatedVNodes = lists:delete(VNode, CoverageVNodes),
            case UpdatedVNodes of
                [] ->
                    finish(clean, StateData);
                _ ->
                    {next_state,
                     waiting_results,
                     StateData#state{coverage_vnodes=UpdatedVNodes},
                     Timeout}
            end;
        false -> % Ignore a response from a VNode that
                 % is not part of the coverage plan
            {next_state,
             waiting_results,
             StateData#state{timeout=Timeout},
             Timeout}
    end;
waiting_results(timeout, StateData) ->
    finish({error, timeout}, StateData).

%% @private
handle_event(_Event, _StateName, State) ->
    {stop, badmsg, State}.

%% @private
handle_sync_event(_Event, _From, StateName, State) ->
    {next_state, StateName, State}.

%% @private
handle_info({'EXIT', _Pid, Reason}, _StateName, StateData) ->
    finish({error, {node_failure, Reason}}, StateData);
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

%% ====================================================================
%% Internal functions
%% ====================================================================

%% @private
finish({error, Error},
       StateData=#state{from={raw, ReqId, ClientPid},
                        client_type=ClientType}) ->
    case ClientType of
        mapred ->
            %% An error occurred or the timeout interval elapsed
            %% so all we can do now is die so that the rest of the
            %% MapReduce processes will also die and be cleaned up.
            exit(Error);
        plain ->
            %% Notify the requesting client that an error
            %% occurred or the timeout has elapsed.
            ClientPid ! {ReqId, Error}
    end,
    {stop,normal,StateData};
finish(clean,
       StateData=#state{from={raw, ReqId, ClientPid},
                        client_type=ClientType}) ->
    case ClientType of
        mapred ->
            luke_flow:finish_inputs(ClientPid);
        plain ->
            ClientPid ! {ReqId, done}
    end,
    {stop,normal,StateData}.
