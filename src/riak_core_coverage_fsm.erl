%% -------------------------------------------------------------------
%%
%% riak_core_coverage_fsm: TODO
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

%% @doc TODO
%%
%%      The keys fsm creates a plan to achieve coverage
%%      of all keys from the cluster using the minimum
%%      possible number of VNodes, sends key listing
%%      commands to each of those VNodes, and compiles the
%%      responses.
%%
%%      The number of VNodes required for full
%%      coverage is based on the number
%%      of partitions, the number of available physical
%%      nodes, and the bucket n_val.

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
-export([test_link/7, test_link/5]).
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
     {process_results, 4}
    ];
behaviour_info(_) ->
    undefined.

-type req_id() :: non_neg_integer().
-type bucket() :: binary().
-type filter() :: fun() | [mfa()].
-type modfun() :: {module(), fun()}.
-type from() :: {raw, req_id(), pid()}.

-record(state, {args :: [{atom(), pos_integer()}],
                bucket :: bucket(),
                client_type :: atom(),
                coverage_factor :: pos_integer(),
                filter :: any(),
                from :: from(),
                mod :: atom(),
                modfun :: modfun(),
                required_responses :: pos_integer(),
                response_count=0 :: non_neg_integer(),
                timeout :: timeout()
               }).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Start a riak_core_coverage_fsm.
%% @deprecated Only in place for backwards compatibility.
%% Please use start_link/5.
%% start_link(ReqId, Input, Timeout, ClientType, _ErrorTolerance, From) ->
%%     case Input of
%%         {filter, Bucket, Filter} ->
%%             ok;
%%         {Bucket, Filter} ->
%%             ok;
%%         _ ->
%%             Bucket = Input,
%%             Filter = none
%%     end,
%%     start_link({raw, ReqId, From}, Bucket, Filter, Timeout, ClientType).

%% @doc Start a riak_core_coverage_fsm
-spec start_link(module(), req_id(), bucket(), filter(), timeout(), atom(), pid()) ->
                        {ok, pid()} | ignore | {error, term()}.
start_link(Mod, ReqId, Bucket, Filter, Timeout, ClientType, From) ->
    start_link(Mod, {raw, ReqId, From}, Bucket, Filter, Timeout, ClientType).

%% @doc Start a riak_core_coverage_fsm.
-spec start_link(module(), from(), bucket(), filter(), timeout(), atom()) ->
                        {ok, pid()} | ignore | {error, term()}.
start_link(Mod, From, Bucket, Filter, Timeout, ClientType) ->
    gen_fsm:start_link(?MODULE,
                  [Mod, From, Bucket, Filter, Timeout, ClientType], []).

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
test_link(Mod, ReqId, Bucket, Filter, R, Timeout, From, StateProps) ->
    test_link(Mod, {raw, ReqId, From}, Bucket, Filter, [{r, R}, {timeout, Timeout}], StateProps).

test_link(Mod, From, Bucket, Filter, _Options, StateProps) ->
    Timeout = 60000,
    ClientType = plain,
    gen_fsm:start_link(?MODULE, {test, [Mod, From, Bucket, Filter, Timeout, ClientType], StateProps}, []).

-endif.

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

%% @private
init([Mod, From={raw, _, ClientPid}, Bucket, Filter, Timeout, ClientType]) ->
    {ok, ModFun, Args, CoverageFactor} = Mod:init(),
    StateData = #state{args=Args,
                       bucket=Bucket,
                       client_type=ClientType,
                       coverage_factor=CoverageFactor,
                       filter=Filter,
                       from=From,
                       mod=Mod,
                       modfun=ModFun,
                       timeout=Timeout},
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
                                      coverage_factor=CoverageFactor,
                                      filter=Filter,
                                      from={_, ReqId, _},
                                      modfun=ModFun,
                                      timeout=Timeout}) ->
    Request = ?COVERAGE_REQ{args=Args,
                            bucket=Bucket,
                            caller=self(),
                            filter=Filter,
                            modfun=ModFun,
                            req_id=ReqId},
    %% TODO: Review use of riak_kv_vnode_master as final parameter.
    case riak_core_vnode_master:coverage(Request, CoverageFactor, riak_kv_vnode_master) of
        {ok, RequiredResponseCount} ->
            StateData = StateData0#state{required_responses=RequiredResponseCount},
            {next_state, waiting_results, StateData, Timeout};
        {error, Reason} ->
            finish({error, Reason}, StateData0)
    end.

%% @private
waiting_results({ReqId, {results, _VNode, Results}},
           StateData=#state{bucket=Bucket,
                            client_type=ClientType,
                            from=From={raw, ReqId, _},
                            mod=Mod,
                            timeout=Timeout}) ->
    Mod:process_results(Results, Bucket, ClientType, From),
    {next_state, waiting_results, StateData, Timeout};
waiting_results({ReqId, _VNode, done}, StateData0=#state{from={raw, ReqId, _},
                                                    required_responses=RequiredResponses,
                                                    response_count=ResponseCount,
                                                    timeout=Timeout}) ->
    ResponseCount1 = ResponseCount + 1,
    StateData = StateData0#state{response_count=ResponseCount1},
    case ResponseCount1 >= RequiredResponses of
        true -> finish(clean, StateData);
        false -> {next_state, waiting_results, StateData, Timeout}
    end;
waiting_results(timeout, StateData) ->
    finish({error, timeout}, StateData).

%% @private
handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

%% @private
handle_info({'EXIT', _Pid, Reason}, _StateName, StateData) ->
    finish({error, {node_failure, Reason}}, StateData);
handle_info({_ReqId, {ok, _Pid}}, StateName, StateData=#state{timeout=Timeout}) ->
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
finish({error, Error}, StateData=#state{from={raw, ReqId, ClientPid}, client_type=ClientType}) ->
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
finish(clean, StateData=#state{from={raw, ReqId, ClientPid}, client_type=ClientType}) ->
    case ClientType of
        mapred ->
            luke_flow:finish_inputs(ClientPid);
        plain ->
            ClientPid ! {ReqId, done}
    end,
    {stop,normal,StateData}.
