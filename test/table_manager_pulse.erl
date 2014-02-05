-module(table_manager_pulse).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-ifdef(PULSE).
-include_lib("pulse/include/pulse.hrl").
-include_lib("pulse_otp/include/pulse_otp.hrl").
-compile({parse_transform, pulse_instrument}).
-compile({pulse_skip, [
%    {wait_for_exit,1},
%    {maybe_kill,1}
%    {claim_test_,0}%,
%    {prop_claims,0},
%    {start_claimant,1},
%    {kill_claimant,2},
%    {restart_manager,1}
]}).
%-compile({pulse_side_effect, [
%    {riak_core_table_manager, wait_for_table_transfer, '_'}
%]}).
-compile(export_all).

-define(tables, [a,b,c,d,e]).

% Root process acts as a supervisor, restarting a new process when it
% exits. The purpose of this test is to ensure the table manager doesn't
% deny a legit claim, and does allow the new claimant to occur once it
% gets the ets transfer message from the first child process' exit.
prop_super_restart() ->
    ?FORALL(Seed, pulse:seed(), begin
        pulse:run(fun() ->
            process_flag(trap_exit, true),
            Self = self(),
            maybe_kill_table_manager(),
            TableConfs = lists:map(fun(N) ->
                {N, [named_table, public]}
            end, ?tables),
            {ok, _Pid} = riak_core_table_manager:start_link(TableConfs),
            A = spawn_link(fun() ->
                {ok, _} = riak_core_table_manager:claim_table(a),
                Self ! claimed,
                receive _ -> ok end
            end),
            receive claimed -> ok end,
            A ! normal,
            receive
                {'EXIT', A, normal} -> ok
            end,
            B = spawn_link(fun() ->
                {ok, _} = riak_core_table_manager:claim_table(a),
                Self ! claimed,
                receive _ -> ok end
            end),
            receive claimed -> ok end,
            Owner = ets:info(a, owner),
            Owner == B
        end, [{seed, Seed}])
    end).

prop_super_restart_test_() ->
    {timeout, 60000, ?_assert(eqc:quickcheck(prop_super_restart()))}.

prop_table_manager_restart() ->
    maybe_kill_table_manager(),
    ?FORALL(Seed, pulse:seed(), begin
        pulse:run(fun() ->
            process_flag(trap_exit, true),
            maybe_kill_table_manager(),
            TableConfs = lists:map(fun(N) ->
                {N, [named_table, public]}
            end, ?tables),
            {ok, _Pid} = riak_core_table_manager:start_link(TableConfs),
            pulse:format("Starting claiming kids.~n"),
            _A = spawn_claimant(a),
            _B = spawn_claimant(b),
            pulse:format("both children have claimed, killing table manager~n"),
            OldMgrPid = whereis(riak_core_table_manager),
            exit(OldMgrPid, kill),
            receive {'EXIT', OldMgrPid, _} -> ok end,
            {ok, _} = riak_core_table_manager:start_link(TableConfs),
            pulse:format("Waiting for children to be alerted that mgr has restarted~n"),
            receive reheired -> ok end,
            receive reheired -> ok end,
            pulse:format("checking that heir is set correctly~n"),
            HeirA = ets:info(a, heir),
            HeirB = ets:info(b, heir),
            MgrPid = whereis(riak_core_table_manager),
            HeirA == MgrPid andalso HeirB == MgrPid
        end, [{seed, Seed}])
    end).

spawn_claimant(TableName) ->
    Self = self(),
    Pid = spawn_link(fun() ->
        {ok, TableData} = riak_core_table_manager:claim_table(TableName),
        pulse:format("Succesfully claimed ~p~n", [TableName]),
        Self ! claimed,
        receive
            {riak_core_table_manager, restarted, TableData} ->
                riak_core_table_manager:ensure_heired(TableData)
        end,
        Self ! reheired,
        receive
            _ -> ok
        end
    end),
    receive claimed -> Pid end.

prop_table_manager_restart_test_() ->
    {timeout, 60000, ?_assert(eqc:quickcheck(prop_table_manager_restart()))}.

%% =====
%% Internal functions
%% =====

maybe_kill_table_manager() ->
    maybe_kill(riak_core_table_manager).

maybe_kill(undefined) ->
    ok;

maybe_kill(Atom) when is_atom(Atom) ->
    maybe_kill(whereis(Atom));

maybe_kill(Pid) when is_pid(Pid) ->
    unlink(Pid),
    exit(Pid, kill),
    wait_for_exit(Pid).

wait_for_exit(Pid) ->
    Mon = erlang:monitor(process, Pid),
    pulse:format("The mon for ~p is ~p~n", [Pid, Mon]),
    receive
        {'DOWN', Mon, process, Pid, _} ->
            pulse:format("Whoohoo!~n"),
            ok
    end.

start_table_manager() ->
    TableConfs = lists:map(fun(Name) ->
        {Name, [named_table, public]}
    end, ?tables),
    riak_core_table_manager:start_link(TableConfs).

get_claim_result(Reporter) ->
    receive
        {claim_result, Reporter, Res} ->
            Res
    end.

extract_pid({Pid, _}) -> Pid.

start_claim_pid(TableName) ->
    Self = self(),
    spawn_link(?MODULE, claim_holder_init, [Self, TableName]).

claim_holder_init(ReportTo, Table) ->
    case riak_core_table_manager:claim_table(Table) of
        {ok, Data} ->
            register(Table, self()),
            ReportTo ! {claim_result, self(), ok},
            claim_holder_loop(Table, Data);
        _Else ->
            ReportTo ! {claim_result, self(), claimed}
    end.

claim_holder_loop(Table, Data) ->
    receive
        Msg ->
            pulse:format("Msg: ~p~n", [Msg]),
            claim_holder_loop(Table, Data)
    end.

-endif.
-endif.
-endif.
