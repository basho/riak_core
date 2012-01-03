%% -------------------------------------------------------------------
%%
%% core_vnode_eqc: QuickCheck tests for riak_core_vnode code
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

%% @doc  QuickCheck tests for riak_core_vnode code

-module(core_vnode_eqc).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_core_vnode.hrl").
-compile([export_all]).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-record(qcst, {started,
               counters, % Dict of counters for each index
               asyncdone_pid, % Collector process for async workers
               indices,
               crash_reasons,
               async_enabled = false,
               async_size = 0,
               async_work=[]}). % {Index, AsyncRef} async work submitted to each vnode

simple_test_() ->
    {setup,
     fun setup_simple/0,
     fun(OldVars) ->
             riak_core_ring_manager:stop(),
             [ok = application:set_env(riak_core, K, V) || {K,V} <- OldVars],
             ok
     end,
     {timeout, 120,
      ?_assertEqual(true, quickcheck(?QC_OUT(numtests(100, prop_simple()))))}}.

setup_simple() ->
    Vars = [{ring_creation_size, 8},
            {ring_state_dir, "<nostore>"},
            {cluster_name, "test"}],
    OldVars = [begin
                   Old = app_helper:get_env(riak_core, AppKey),
                   ok = application:set_env(riak_core, AppKey, Val),
                   {AppKey, Old}
               end || {AppKey, Val} <- Vars],
    riak_core_ring_events:start_link(),
    riak_core_ring_manager:start_link(test),
    riak_core_vnode_proxy_sup:start_link(),
    riak_core:register([{vnode_module, mock_vnode}]),
    OldVars.

test(N) ->
    quickcheck(numtests(N, prop_simple())).

prop_simple() ->
    ?FORALL(Cmds, commands(?MODULE, {setup, initial_state_data()}),
            aggregate(command_names(Cmds),
                      begin
                          {H,{_SN,S},Res} = run_commands(?MODULE, Cmds),
                          %timer:sleep(100), %% Adjust this to make shutdown sensitive stuff pass/fail
                          %% Do a sync operation on all the started vnodes
                          %% to ensure any of the noreply commands have executed before
                          %% stopping.
                          get_all_counters(S#qcst.started),
                          stop_servers(),
                          %% Check results
                          ?WHENFAIL(
                             begin
                                 io:format(user, "History: ~p\n", [H]),
                                 io:format(user, "State: ~p\n", [S]),
                                 io:format(user, "Result: ~p\n", [Res])
                             end,
                             conjunction([{res, equals(Res, ok)},
                                          {async, 
                                           equals(lists:sort(async_work(S#qcst.asyncdone_pid)),
                                                  lists:sort(filter_work(S#qcst.async_work,
                                                      S#qcst.asyncdone_pid)))}]))
                      end)).

active_index(#qcst{started=Started}) ->
    elements(Started).

%% Generate a preflist element
active_preflist1(S) ->
    {active_index(S), node()}.

%% Generate a preflist - making sure the partitions are unique
active_preflist(S) ->
    non_empty(?SUCHTHAT(Xs,list(active_preflist1(S)),lists:sort(Xs)==lists:usort(Xs))).

%% Generate the async pool size
gen_async_pool() ->
    oneof([0, 1, 10]).

initial_state() ->
    stopped.

index(S) ->
    oneof(S#qcst.indices).

initial_state_data() ->
    Ring = riak_core_ring:fresh(8, node()),
    riak_core_ring_manager:set_my_ring(Ring),
    #qcst{started=[],
          counters=orddict:new(),
          crash_reasons=orddict:new(),
          indices=[I || {I,_N} <- riak_core_ring:all_owners(Ring)]
         }.

%% Setup new test run - start new async work collector and set the async pool size
next_state_data(_From,_To,S,_R,
                {call,?MODULE,enable_async,[AsyncSize]}) ->
    S#qcst{async_enabled = true,
           async_size = AsyncSize};
%% Setup new test run - start new async work collector and set the async pool size
next_state_data(_From,_To,S,AsyncDonePid,
                {call,?MODULE,prepare,[_AsyncSize]}) ->
    S#qcst{asyncdone_pid = AsyncDonePid};
%% Mark the vnode as started
next_state_data(_From,_To,S=#qcst{started=Started,
                                  counters=Counters,
                                  crash_reasons=CRs},_R,
                {call,?MODULE,start_vnode,[Index]}) ->
    S#qcst{started=[Index|Started],
           counters=orddict:store(Index, 0, Counters),
           crash_reasons=orddict:store(Index, undefined, CRs)};
next_state_data(_From,_To,S=#qcst{started=Started, counters=Counters, crash_reasons=CRs},_R,
                {call,mock_vnode,stop,[{Index,_Node}]}) ->
    %% If a node is stopped, reset the counter ready for next
    %% time it is called which should start it

    %% give the vnode a chance to shut down so that the index isn't present
    %% if the next command is to list the vnodes
    timer:sleep(10),
    S#qcst{started=Started -- [Index],
           counters=orddict:store(Index, 0, Counters),
           crash_reasons=orddict:store(Index, undefined, CRs)};
%% Update the counters for the index if a command that changes them
next_state_data(_From,_To,S=#qcst{counters=Counters},_R,
                {call,_Mod,Func,[Preflist]})
  when Func =:= neverreply; Func =:= returnreply; Func =:= latereply ->
    S#qcst{counters=lists:foldl(fun({I, _N}, C) ->
                                        orddict:update_counter(I, 1, C)
                                end, Counters, Preflist)};
%% Update the counters for the index if a command that changes them
next_state_data(_From,_To,S=#qcst{crash_reasons=CRs},_R,
                {call,mock_vnode,crash,[{Index,_Node}]}) ->
    S#qcst{crash_reasons=orddict:store(Index, Index, CRs)};
%% Update the expected async work
next_state_data(_From,_To,S=#qcst{counters=Counters,
                                  async_work=Work}, R,
                {call,_Mod,Func,[Preflist, _AsyncDonePid]})
  when Func =:= asyncnoreply; Func =:= asyncreply; Func =:= asynccrash ->
    NewWork = [{Idx, R} || {Idx, _N} <- Preflist],
    S2=S#qcst{async_work=Work ++ NewWork,
           counters=lists:foldl(fun({I, _N}, C) ->
                                        orddict:update_counter(I, 1, C)
                                end, Counters, Preflist)},
    %% io:format(user, "S2=~p\n", [S2]),
    S2;
next_state_data(_From,_To,S,_R,_C) ->
    S.
% 

setup(S) ->
    [{setup,   {call,?MODULE,enable_async,[gen_async_pool()]}},
     {stopped, {call,?MODULE,prepare,[S#qcst.async_size]}}].

stopped(S) ->
    [{running, {call,?MODULE,start_vnode,[index(S)]}}].

running(S) ->
    [
     {history, {call,?MODULE,start_vnode,[index(S)]}},
     {history, {call,mock_vnode,get_index,[active_preflist1(S)]}},
     {history, {call,mock_vnode,get_counter,[active_preflist1(S)]}},
     {history, {call,mock_vnode,crash,[active_preflist1(S)]}},
     {history, {call,mock_vnode,get_crash_reason,[active_preflist1(S)]}},
     {history, {call,mock_vnode,neverreply,[active_preflist(S)]}},
     {history, {call,?MODULE,returnreply,[active_preflist(S)]}},
     {history, {call,?MODULE,latereply,[active_preflist(S)]}},
     {history, {call,mock_vnode,asyncnoreply,[active_preflist(S),
                                              S#qcst.asyncdone_pid]}},
     {history, {call,?MODULE,asyncreply,[active_preflist(S),
                                         S#qcst.asyncdone_pid]}},
     {history, {call,?MODULE,asynccrash,[active_preflist(S),
                                         S#qcst.asyncdone_pid]}},
     {history, {call,?MODULE,restart_master,[]}},
     {history, {call,mock_vnode,stop,[active_preflist1(S)]}},
     {history, {call,riak_core_vnode_master,all_nodes,[mock_vnode]}}
    ].

precondition(_From,_To,#qcst{started=Started},{call,?MODULE,start_vnode,[Index]}) ->
    not lists:member(Index, Started);
precondition(_From,_To,#qcst{started=Started},{call,_Mod,Func,[Preflist]}) 
  when Func =:= get_index; Func =:= get_counter; Func =:= neverreply; Func =:= returnreply;
       Func =:= latereply; Func =:= crash; Func =:= get_crash_reason ->
    preflist_is_active(Preflist, Started);
precondition(_From,_To,#qcst{started=Started,async_size=AsyncSize},
             {call,_Mod,Func,[Preflist, _DonePid]}) 
  when Func =:= asyncnoreply; Func =:= asyncreply; Func =:= asynccrash ->
    preflist_is_active(Preflist, Started) andalso AsyncSize > 0;
precondition(_From,_To,_S,_C) ->
    true.

postcondition(_From,_To,_S,
              {call,mock_vnode,get_index,[{Index,_Node}]},{ok,ReplyIndex}) ->
    Index =:= ReplyIndex;
postcondition(_From,_To,#qcst{crash_reasons=CRs},
              {call,mock_vnode,get_crash_reason,[{Index,_Node}]},{ok, Reason}) ->
    %% there is the potential for a race here if get_crash_reason is called
    %% before the EXIT signal is sent to the vnode, but it didn't appear 
    %% even with 1k tests - just a note in case a heisenbug rears its head
    %% on some future, less deterministic day.
    orddict:fetch(Index, CRs) =:= Reason;
postcondition(_From,_To,#qcst{counters=Counters},
              {call,mock_vnode,get_counter,[{Index,_Node}]},{ok,ReplyCount}) ->
    orddict:fetch(Index, Counters) =:= ReplyCount;
postcondition(_From,_To,_S,
              {call,_Mod,Func,[]},Result)
  when Func =:= neverreply; Func =:= returnreply; Func =:= latereply ->
    Result =:= ok;
postcondition(_From,_To,_S,
              {call,riak_core_vnode_master,all_nodes,[mock_vnode]},Result) ->
    Pids = [Pid || {_,Pid,_,_} <- supervisor:which_children(riak_core_vnode_sup)],
    lists:sort(Result) =:= lists:sort(Pids);
postcondition(_From,_To,_S,_C,_R) ->
    true.

%% Pre/post condition helpers

preflist_is_active({Index,_Node}, Started) ->
    lists:member(Index, Started);
preflist_is_active(Preflist, Started) ->
    lists:all(fun({Index,_Node}) -> lists:member(Index, Started) end, Preflist).

%% Get the counters from running vnodes
get_all_counters(Started) ->
    [{I, mock_vnode:get_counter({I, node()})} || I <- Started].

%% Local versions of commands

%% Enable async tests, triggers state change in next_state so precondition can test
enable_async(_AsyncSize) ->
    ok.

%% Prepare to run vnode tests, do any setup needed
prepare(AsyncSize) ->
    application:set_env(riak_core, core_vnode_eqc_pool_size, AsyncSize),
    start_servers(),
    proc_lib:spawn_link(
      fun() -> 
              %% io:format(user, "Starting async work collector ~p\n", [self()]),
              async_work_proc([], []) 
      end).


start_vnode(I) ->
    ok = mock_vnode:start_vnode(I).

returnreply(Preflist) ->
    {ok, Ref} = mock_vnode:returnreply(Preflist),
    check_receive(length(Preflist), returnreply, Ref).

latereply(Preflist) ->
    {ok, Ref} = mock_vnode:latereply(Preflist),
    check_receive(length(Preflist), latereply, Ref).
    
asyncreply(Preflist, AsyncDonePid) ->
    {ok, Ref} = mock_vnode:asyncreply(Preflist, AsyncDonePid),
    check_receive(length(Preflist), asyncreply, Ref),
    {ok, Ref}.
asynccrash(Preflist, AsyncDonePid) ->
    {ok, Ref} = mock_vnode:asynccrash(Preflist, AsyncDonePid),
    check_receive(length(Preflist), 
                  {worker_crash, deliberate_async_crash, {crash, AsyncDonePid}}, 
                  Ref),
    {ok, Ref}.
           
check_receive(0, _Msg, _Ref) ->
    ok;
check_receive(Replies, Msg, Ref) ->
    receive
        {Ref, Msg} ->
            check_receive(Replies-1, Msg, Ref);
        {Ref, OtherMsg} ->
            {error, {bad_msg, Msg, OtherMsg}}
    after
        1000 ->
            {error, timeout}
    end.

%% Server start/stop infrastructure

start_servers() ->
    stop_servers(),
    {ok, _Sup} = riak_core_vnode_sup:start_link(),
    {ok, _} = riak_core_vnode_manager:start_link(),
    {ok, _VMaster} = riak_core_vnode_master:start_link(mock_vnode).

stop_servers() ->
    %% Make sure VMaster is killed before sup as start_vnode is a cast
    %% and there may be a pending request to start the vnode.
    stop_pid(whereis(mock_vnode_master)),
    stop_pid(whereis(riak_core_vnode_manager)),
    stop_pid(whereis(riak_core_vnode_sup)).

restart_master() ->
    %% Call get status to make sure the riak_core_vnode_master
    %% has processed any commands that were cast to it.  Otherwise
    %% commands like neverreply are not cast on to the vnode and the
    %% counters are not updated correctly.
    sys:get_status(mock_vnode_master),
    stop_pid(whereis(mock_vnode_master)),
    {ok, _VMaster} = riak_core_vnode_master:start_link(mock_vnode).

stop_pid(undefined) ->
    ok;
stop_pid(Pid) ->
    unlink(Pid),
    exit(Pid, shutdown),
    ok = wait_for_pid(Pid).

wait_for_pid(Pid) ->
    Mref = erlang:monitor(process, Pid),
    receive
        {'DOWN',Mref,process,_,_} ->
            ok
    after
        5000 ->
            {error, didnotexit}
    end.

%% Async work collector process - collect all messages until work requested
async_work_proc(AsyncWork, Crashes) ->
    receive
        {crashes, Pid} ->
            Pid ! {crashes, Crashes},
            async_work_proc(AsyncWork, Crashes);
        {get, Pid} ->
            Pid ! {work, lists:reverse(AsyncWork)},
            async_work_proc(AsyncWork, Crashes);
        {_, {error, _}} = Crash ->
            async_work_proc(AsyncWork, [Crash|Crashes]);
        {_, asyncreply} ->
            %% this is a legitimate reply, ignore it
            async_work_proc(AsyncWork, Crashes);
        Work ->
            async_work_proc([Work | AsyncWork], Crashes)
    end.

%% Request async work completed
async_work(undefined) ->
%%    io:format(user, "Did not get as far as setting up async worker\n", []),
    [];
async_work(Pid) ->
%%    io:format(user, "Getting async work from ~p\n", [Pid]),
    Pid ! {get, self()},
    receive
        {work, Work} ->
            Work
    after
        1000 ->
            throw(async_work_timeout)
    end.

%% Request async work crashes
async_crashes(undefined) ->
%%    io:format(user, "Did not get as far as setting up async worker\n", []),
    [];
async_crashes(Pid) ->
%%    io:format(user, "Getting async crashes from ~p\n", [Pid]),
    Pid ! {crashes, self()},
    receive
        {crashes, Crashes} ->
            Crashes
    after
        1000 ->
            throw(async_crashes_timeout)
    end.


filter_work(Work, Pid) ->
    Crashes = async_crashes(Pid),
    CompletedWork = async_work(Pid),
    CrashRefs = lists:map(fun({Tag, _error}) -> Tag end, Crashes),
    case Pid of
        undefined -> ok;
        _ ->
            unlink(Pid),
            exit(Pid, kill)
    end,
    lists:filter(fun({_Index, {_Reply, Tag}}=WorkItem) ->
                case lists:member(Tag, CrashRefs) of
                    true ->
                        %% this isn't quite straightforward as a request can
                        %% apparently go to multiple vnodes. We have to make
                        %% sure we're only removing crashes from vnodes that
                        %% didn't reply
                        lists:member(WorkItem, CompletedWork);
                    _ -> true
                end
        end, Work).

-endif.

