%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc riak_core_claim_sim models adding/removing nodes from the ring
%%
-module(riak_core_claim_sim).
-compile(export_all).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(simopts,  {wants,
                   choose,
                   prepare = finish,
                   current = fun(_Ring) -> ok end,
                   prepared = fun(_Ring, _Prepare) -> ok end,
                   percmd = fun(_Ring, _Cmd) -> ok end,
                   postxfer = fun(_Ring) -> ok end,
                   rebalance = fun(_R1, _R2) -> ok end,
                   analysis = fun(_Ring, _Cmds) -> ok end,
                   return_ring = false}).

help() ->
    io:format("\n"
              "Claim Simulator - simulates effect of join/leave commands\n"
              "\n"
              "  riak_core_claim_sim:run([Opts]).\n"
              "\n"
              "Opts = {claimant, node()} | \n"
              "       {ringfile, path()} | \n"
              "       {ring, ring()} | {appvars, [{Key,Val}]} \n"
              "\n"
              "       {wants, {WMod,WFun}}\n"
              "       {choose, {CMod,CFun} | {CMod,CFun,CParam}}\n"
              "       {target_n_val, int()}\n"
              "       {prepare, finish | cancel}\n"
              "       {print, false | standard_io | io_device()}\n"
              "       {cmds, [Commands]}\n"
              "\n"
              "Commands = {join, node()} | {join, integer()}} | {leave, node()} | [Commands]\n"
              "\n"
              "The simulator uses the options to select the ring to use\n"
              "  if claimant is set, grab the ring and target n-val from that node\n"
              "  if ringfile is set, load the ring file from disk\n"
              "  if neither set, use the current ring from the node\n"
              "\n"
              "Any active transfers are handled with the prepare option.  The default is to \n"
              "finish the transfers, but they can be cancelled with {prepare, cancel}\n"
              "\n"
              "The cmds list provides the cluster transitions - commands can either be individual\n"
              "join/leave commands or a batch to be added all at once.\n"
              "{cmds, [[{join, 'riak@hosta'},{join, 'riak@hostb'}],{join,'riak@hostc'}]}\n"
              "would join hosta/hostb and recalculate claim, then join hostc.\n"
              "\n"
              "Caveat: To use leave in commands, target_n_val must be set in the app env.\n"
              "\n"
              "Examples:\n"
              "  Simulate adding 9 nodes in a single batch to the current ring\n"
              "     riak_core_claim_sim:run([{cmds, [[{join, 9}]]}]).\n"
              "\n"
              "  Simulate cancelling transfers, adding a node then and removing a different node\n"
              "     riak_core_claim_sim:run([{cmds, [{join, 'riak@newnode'},\n"
              "                                      {leave, 'riak@oldnode'}]},\n"
              "                              {prepare, cancel}]).\n"
              "\n").

run([Config]) when is_list(Config) ->
    io:format("Loading config: ~s~n", [Config]),
    case file:consult(Config) of
        {ok, Terms} ->
            run(Terms);
        {error, Reason} ->
            {cannot_read_config, Reason}
        end;
run(Opts) ->
    Claimant = proplists:get_value(claimant, Opts),
    RingFile = proplists:get_value(ringfile, Opts),
    RingArg = proplists:get_value(ring, Opts),
    
    AppVars = proplists:get_value(riak_core, Opts, []),
    setup_environment(AppVars),
               
    Wants = proplists:get_value(wants, Opts, 
                                app_helper:get_env(riak_core, wants_claim_fun)),
    Choose = proplists:get_value(choose, 
                                 Opts, app_helper:get_env(riak_core, choose_claim_fun)),
    
    Prepare = proplists:get_value(prepare, Opts, finish),
    Cmds = proplists:get_value(cmds, Opts, []),
    IoDev = proplists:get_value(print, Opts, standard_io),
    Analysis = proplists:get_value(analysis, Opts, []),
    ReturnRing = proplists:get_value(return_ring, Opts, false),
    
    TN0 = proplists:get_value(target_n_val, Opts, app_helper:get_env(riak_core, target_n_val)),

    {Ring, TN} = 
        case {Claimant, RingFile, RingArg} of
            {undefined, undefined, undefined} ->
                {ok, Ring0} = riak_core_ring_manager:get_raw_ring(),
                {Ring0, TN0};
            {Claimant, undefined, undefined} ->
                {ok, Ring0} = rpc:call(Claimant, riak_core_ring_manager, get_raw_ring, []),
                ClaimantTN = rpc:call(Claimant, app_helper, get_env, [riak_core, target_n_val]),
                {Ring0, ClaimantTN};
            {undefined, RingFile, undefined} ->
                Ring0 = riak_core_ring_manager:read_ringfile(RingFile),
                {Ring0, TN0};
            {undefined, undefined, RingArg} ->
                {RingArg, TN0};
            _ ->
                help(),
                throw({bad_opts, Opts})
            end,
    Choose1 = add_choose_params(Choose, TN),

    SimOpts0 = default_simopts(IoDev, Analysis, TN),
    SimOpts = SimOpts0#simopts{wants = Wants, choose = Choose1, 
                               prepare = Prepare, return_ring = ReturnRing},
    
    dryrun(Ring, Cmds, SimOpts).

add_choose_params(Choose, TN) ->
    Params = case Choose of
                 {CMod, CFun} ->
                     [];
                 {CMod, CFun, Params0} ->
                     Params0
             end,
    Params1 = [{target_n_val, TN} | proplists:delete(target_n_val, Params)],
    {CMod, CFun, Params1}.

run_rebalance(Ring, Wants, Choose, Rebalance) ->
    Ring2 = riak_core_claim:claim(Ring, Wants, Choose),
    Rebalance(Ring, Ring2),
    Ring2.

read_ringfile(RingFile) ->
    case file:read_file(RingFile) of
        {ok, Binary} ->
            binary_to_term(Binary);
        {error, Reason} ->
            throw({bad_ring, Reason})
    end.

setup_environment(Vars) ->
    _ = [application:set_env(riak_core, Key, Val) || {Key, Val} <- Vars],
    ok.

%% Perform the dry run, if printing is disabled, return the ring
dryrun(Ring, CmdsL0, SimOpts) ->
    CmdsL = case CmdsL0 of 
                [Cmd0|_] when not is_list(Cmd0) ->
                    [[Cmd] || Cmd <- CmdsL0];
                _ ->
                    CmdsL0
            end,
    Ring1 = dryrun1(Ring, CmdsL, SimOpts),
    case SimOpts#simopts.return_ring of
        true ->
            Ring1;
        _ ->
            ok
    end.

make_current(IoDev, TN) ->
    fun(Ring00) ->
            o(IoDev, "Current ring:~n", []),
            pretty_print(IoDev, Ring00, TN)
    end.

make_prepared(IoDev, Analysis, TN) ->
    fun(Ring01, Prepare) ->
            o(IoDev, "~nCurrent after ~p transfers:~n", [Prepare]),
            pretty_print(IoDev, Ring01, TN),
            run_analysis(IoDev, Analysis, Ring01)
    end.

make_postxfer(IoDev, TN) ->
    fun(Ring01) ->
            o(IoDev, "~nAfter transfers:~n", []),
            pretty_print(IoDev, Ring01, TN)
    end.

make_percmd(IoDev) ->
    fun(_Ring, Cmd) ->
            case Cmd of
                ({join, Node}) when is_atom(Node) ->
                    o(IoDev, "Joining ~p~n", [Node]);
                ({join, Count}) when is_integer(Count) ->
                    o(IoDev, "Joining ~p...~p~n",
                      [sim_node(1), sim_node(Count)]);
                ({join, Start, End}) ->
                    o(IoDev, "Joining ~p...~p~n",
                      [sim_node(Start), sim_node(End)]);
                ({leave, Node}) when is_atom(Node) ->
                    o(IoDev, "Leaving ~p~n", [Node])
            end
    end.
   
make_rebalance(false, _TN) ->
    fun(_R1, _R2) -> ok end;
make_rebalance(IoDev, TN) ->
    fun(Ring, Ring2) ->
            o(IoDev, "~nAfter rebalance~n", []),

            Owners1 = riak_core_ring:all_owners(Ring),
            Owners2 = riak_core_ring:all_owners(Ring2),
            Owners3 = lists:zip(Owners1, Owners2),
            Next = [{Idx, PrevOwner, NewOwner}
                    || {{Idx, PrevOwner}, {Idx, NewOwner}} <- Owners3,
                       PrevOwner /= NewOwner],
            Tally =
                lists:foldl(fun({_, PrevOwner, NewOwner}, Tally) ->
                                    dict:update_counter({PrevOwner, NewOwner}, 1, Tally)
                            end, dict:new(), Next),
            pretty_print(IoDev, Ring2, TN),
            o(IoDev, "Pending: ~p~n", [length(Next)]),
            BadPL = riak_core_ring_util:check_ring(Ring2, TN),
            o(IoDev, "Preflists violating targetN=~p: ~p~n", [TN, length(BadPL)]),
            _ = [o(IoDev, "~b transfers from ~p to ~p~n", [Count, PrevOwner, NewOwner])
             || {{PrevOwner, NewOwner}, Count} <- dict:to_list(Tally)],
            ok
    end.

make_analysis(_IoDev, []) ->
    fun(_Ring, _Cmds) -> ok end;
make_analysis(IoDev, Analysis) ->
    fun(Ring, _Cmds) ->
            run_analysis(IoDev, Analysis, Ring)
    end.

default_simopts(IoDev, Analysis, TN) ->
    #simopts{current = make_current(IoDev, TN),
             prepared = make_prepared(IoDev, Analysis, TN),
             percmd = make_percmd(IoDev),
             postxfer = make_postxfer(IoDev, TN),
             rebalance = make_rebalance(IoDev, TN),
             analysis = make_analysis(IoDev, Analysis)}.

dryrun1(Ring00, CmdsL, #simopts{wants = Wants,
                                choose = Choose,
                                prepare = Prepare,
                                current = Current,
                                prepared = Prepared,
                                percmd = PerCmd,
                                postxfer = PostXfer,
                                rebalance = Rebalance,
                                analysis = Analysis}) ->
    Current(Ring00),

    Ring01 = case Prepare of
                 cancel ->
                     riak_core_ring:cancel_transfers(Ring00);
                 finish ->
                     riak_core_ring:future_ring(Ring00)
             end,

    Prepared(Ring01, Prepare),
    Ring0 = run_rebalance(Ring01, Wants, Choose, Rebalance),
    
    lists:foldl(
      fun(Cmds, RingAcc1) ->
              NewRing =
                  lists:foldl(
                    fun(Cmd, RingAcc2) ->
                            RingAcc3 = command(Cmd, RingAcc2),
                            PerCmd(RingAcc3, Cmd),
                            RingAcc3
                    end, RingAcc1, Cmds),
              {_, NewRing2} = riak_core_claimant:reassign_indices(NewRing),
              Pending = riak_core_ring:pending_changes(NewRing2),
              case Pending of
                  [] ->
                      NewRing3 = NewRing2;
                  _ ->
                      NewRing3 = riak_core_ring:future_ring(NewRing2),
                      PostXfer(NewRing3)
              end,
              NewRing4 = run_rebalance(NewRing3, Wants, Choose, Rebalance),
              NewRing5 = run_rebalance(NewRing4, Wants, Choose, Rebalance),

              Analysis(NewRing5, Cmds),
              NewRing5
      end, Ring0, CmdsL).

sim_node(N) ->
    list_to_atom(lists:flatten(io_lib:format("sim~b@127.0.0.1",[N]))).
    
command({join, Num}, Ring) when is_integer(Num) ->
    command({join, 1, Num}, Ring);
command({join, Start, End}, Ring) ->
    lists:foldl(fun(N, RingAcc) ->
                        Node = sim_node(N),
                        command({join, Node}, RingAcc)
                end, Ring, lists:seq(Start, End));

command({join, Node}, Ring) when is_atom(Node) ->
    %% Members = riak_core_ring:all_members(Ring),
    %% (not lists:member(Node, Members)) orelse throw(invalid_member),
    riak_core_ring:add_member(Node, Ring, Node);
command({leave, Node}, Ring) ->
    Members = riak_core_ring:all_members(Ring),
    lists:member(Node, Members) orelse throw(invalid_member),
    riak_core_ring:leave_member(Node, Ring, Node).

%% Pretty print the ring if IoDev is not false
pretty_print(false, _Ring, _TN) ->
    ok;
pretty_print(IoDev, Ring, TN) ->
    riak_core_ring:pretty_print(Ring, [legend, {out, IoDev}, {target_n, TN}]).

%% Run analysis of the ring
run_analysis(false, _Analysis, _Ring) ->
    ok;
run_analysis(IoDev, Analysis, Ring) ->
    case proplists:get_value(failures, Analysis, 0) of
        0 ->
            ok;
        Failures ->
            o(IoDev, "Failure analysis - up to ~p nodes\n", [Failures]),
            NVal = proplists:get_value(n_val, Analysis, 3),
            try
                riak_core_claim_util:print_analysis(IoDev,
                                                    riak_core_claim_util:sort_by_down_fbmax(
                                                      riak_core_claim_util:node_sensitivity(Ring, NVal, Failures)))
            catch
                _:Err ->
                    o(IoDev, "Problem with failure analysis: ~p\n", [Err])
            end
    end.

%% Output to the iodev supplied
o(false, _Fmt) ->
    ok;
o(IoDev, Fmt) ->
    io:format(IoDev, Fmt).

o(false, _Fmt, _Args) ->
    ok;
o(IoDev, Fmt, Args) ->
    io:format(IoDev, Fmt, Args).

%% -------------------------------------------------------------------
%%
%% Commissioning testing - provides a way to compare claim algorithms
%%
%% -------------------------------------------------------------------

%% Run each commission test against each commission claim
commission() ->
    commission(".").

commission(Base) ->
    Claims = commission_claims(),
    _ = [begin
         io:format("~p\n", [Test]),
         commission(Base, Test, Claims)
     end || Test <- commission_tests_first()],
    ok.

commission(Base, Test) ->
    commission(Base, Test, commission_claims()).

commission(Base, Test, Claims) when is_list(Claims) ->
    [try
         commission(Base, Test, Claim)
     catch 
         _:Err ->
             io:format("~p / ~p failed - ~p\n", [Test, Claim, Err]),
             Err
     end || Claim <- Claims];
commission(Base, Test, {Wants, Choose}) ->
    RingSize = proplists:get_value(ring_size, Test, 64),
    Nodes = proplists:get_value(nodes, Test, 16),
    NVal = proplists:get_value(n_val, Test, 3),
    TN = proplists:get_value(target_n_val, Test, 4),
    
    Ring = riak_core_ring:fresh(RingSize, sim_node(1)),
    
    SeqJoinCmds = [ [{join, sim_node(I)}] || I <- lists:seq(2, Nodes)],
    BulkJoinCmds =  [ [ {join, sim_node(I)} || I <- lists:seq(2, Nodes) ] ],

    Dir = commission_test_dir(Base, RingSize, Nodes, NVal, TN, Choose),
    case filelib:is_dir(Dir) of
        true ->
            throw(already_generated);
        _ ->
            ok
    end,
    
    ok = filelib:ensure_dir(filename:join(Dir, empty)),

    {ok, SeqFh} = file:open(filename:join([Dir, "sequential.txt"]), [write]),
    io:format(SeqFh, "cmds,balance,violations,diversity\n", []),
    application:set_env(riak_core, target_n_val, TN),

    Choose1 = add_choose_params(Choose, TN),

    %% Sequential node joins
    %%   initial ring of one node, add up to Nodes sequentially
    SeqSimOpts = #simopts{
      wants = Wants,
      choose = Choose1,
      analysis = 
          fun(Ring2, Cmds) ->
                  Seq = get(sim_seq),
                  put(sim_seq, Seq+1),
                  FN = filename:join([Dir, 
                                      "sj"++integer_to_list(Seq)++".ring" ]),
                  ok = file:write_file(FN, term_to_binary(Ring2)),

                  Stats = try
                              riak_core_claim_util:ring_stats(Ring2, TN)
                          catch
                              _:Reason ->
                                  lager:info("Ring stats failed - ~p\n", [Reason]),
                                  []
                          end,
                  io:format(SeqFh, "\"~w\",~p,~p,~p\n",
                            [Cmds,
                             proplists:get_value(balance, Stats, undefined),
                             proplists:get_value(violations, Stats, undefined),
                             proplists:get_value(diversity, Stats, undefined)])
          end},
    put(sim_seq, 2),
    dryrun(Ring, SeqJoinCmds, SeqSimOpts),
    ok = file:close(SeqFh),

    %% Bulk node joins
    %%   bulk join N nodes to a single node
    %% TODO: Break bulk join up differently - add 10, then another 10

    {ok, BulkFh} = file:open(filename:join([Dir, "bulk.txt"]), [write]),
    io:format(BulkFh, "cmds,balance,violations,diversity\n", []),

    BulkSimOpts = #simopts{
      wants = Wants,
      choose = Choose,
      analysis = 
          fun(Ring2, Cmds) ->
                  Seq = get(sim_seq),
                  put(sim_seq, Seq+1),
                  FN = filename:join([Dir, 
                                      "bk"++integer_to_list(Nodes)++".ring" ]),
                  ok = file:write_file(FN, term_to_binary(Ring2)),

                  Stats = try
                              riak_core_claim_util:ring_stats(Ring2, TN)
                          catch
                              _:Reason ->
                                  lager:info("Ring stats failed - ~p\n", [Reason]),
                                  []
                          end,
                  io:format(BulkFh, "\"~w\",~p,~p,~p\n",
                            [Cmds,
                             proplists:get_value(balance, Stats, undefined),
                             proplists:get_value(violations, Stats, undefined),
                             proplists:get_value(diversity, Stats, undefined)])
          end},
    put(sim_seq, 2),
    dryrun(Ring, BulkJoinCmds, BulkSimOpts),
    file:close(BulkFh).

commission_test_dir(Base, RingSize, Nodes, NVal, TN, {_Mod, Choose}) ->
    filename:join(Base,
                  lists:flatten(io_lib:format("q~b_s~b_n~b_t~b_~p", 
                                              [RingSize, Nodes, NVal, TN,
                                               Choose]))).


commission_tests_all() ->
    First = commission_tests_first(),
    Rest = commission_tests_rest(),
    First ++ (Rest -- First).

commission_tests_first() ->
    [[{n_val, 3}, {target_n_val, 3}, {nodes, 32}, {ring_size, 64}],
     [{n_val, 3}, {target_n_val, 4}, {nodes, 32}, {ring_size, 64}],
     [{n_val, 3}, {target_n_val, 3}, {nodes, 32}, {ring_size, 1024}],
     [{n_val, 3}, {target_n_val, 4}, {nodes, 32}, {ring_size, 1024}],
     [{n_val, 3}, {target_n_val, 3}, {nodes, 32}, {ring_size, 256}],
     [{n_val, 3}, {target_n_val, 4}, {nodes, 32}, {ring_size, 256}],
     [{n_val, 1}, {target_n_val, 1}, {nodes, 32}, {ring_size, 64}],
     [{n_val, 1}, {target_n_val, 1}, {nodes, 32}, {ring_size, 256}],
     [{n_val, 1}, {target_n_val, 1}, {nodes, 32}, {ring_size, 1024}],
     [{n_val, 5}, {target_n_val, 5}, {nodes, 32}, {ring_size, 64}],
     [{n_val, 5}, {target_n_val, 5}, {nodes, 32}, {ring_size, 256}],
     [{n_val, 5}, {target_n_val, 5}, {nodes, 32}, {ring_size, 1024}]].

commission_tests_rest() ->
    [[{n_val, NVal}, {target_n_val, NVal+TNDelta}, {nodes, min(Q, 64)}, {ring_size, Q}] ||
        Q <- [64, 256, 1024, 32, 128, 512, 2048, 4096, 8192, 16384],
        NVal <- lists:seq(1, 9), 
        TNDelta <- lists:seq(0, 3)
        ].

commission_claims() ->
    [{{riak_core_claim, wants_claim_v1}, {riak_core_claim, choose_claim_v1}},
     {{riak_core_claim, wants_claim_v2}, {riak_core_claim, choose_claim_v2}},
     {{riak_core_claim, wants_claim_v3}, {riak_core_claim, choose_claim_v3}}].


%% -------------------------------------------------------------------
%% Unit Tests
%% -------------------------------------------------------------------

-ifdef(TEST).

run_test() ->
    Ring = riak_core_ring:fresh(64, anode),
    ?assertEqual(ok, run([{ring, Ring},
                          {target_n_val,2},
                          {wants,{riak_core_claim,wants_claim_v2}},
                          {choose,{riak_core_claim,choose_claim_v2}},
                          {cmds, [[{join,a}],[{join,b}]]},
                          {print,false},
                          {return_ring, false}])),
    Ring2 = run([{ring, Ring},
                 {target_n_val,2},
                 {wants,{riak_core_claim,wants_claim_v2}},
                 {choose,{riak_core_claim,choose_claim_v2}},
                 {cmds, [[{join,a}],[{join,b}]]},
                 {print,false},
                 {return_ring, true}]),
    ?assert(is_tuple(Ring2)),
    {ok, Fh} = file:open("sim.out", [write]),
    ?assertEqual(ok, run([{ring, Ring2},
                          {target_n_val,4},
                          {wants,{riak_core_claim,wants_claim_v1}},
                          {choose,{riak_core_claim,choose_claim_v1}},
                          {cmds, [[{join,3}]]},
                          {analysis, [{failures, 2},{n_val, 3}]},
                          {print,Fh},
                          {return_ring, false}])),
    file:close(Fh).


%% Decided not to run by default, perhaps better as an
%% integration test.
commission_test_no_longer_run_by_default() ->
    {timeout, 120, 
     ?_test(begin
                Dir = "commission_test",
                os:cmd("rm -rf " ++ Dir),
                filelib:ensure_dir(filename:join([Dir, "empty"])),
                Claims = commission_claims(),
                [ok] = lists:usort(commission(Dir,
                                              [{n_val, 3}, {target_n, 4}, 
                                               {nodes, 5}, {ring_size, 64}],
                                              Claims)),
                %% Check each claim has a sequential and bulk file written
                [begin
                     TD = commission_test_dir(Dir, 64, 5, 3, 4, Choose),
                     ?assert(filelib:is_dir(TD)),
                     ?assert(filelib:is_file(filename:join(TD, "sequential.txt"))),
                     ?assert(filelib:is_file(filename:join(TD, "sj5.ring"))),
                     ?assert(filelib:is_file(filename:join(TD, "bulk.txt"))),
                     ?assert(filelib:is_file(filename:join(TD, "bk5.ring")))
                 end|| 
                    {_Want, Choose} <- Claims]
            end)}.

-endif. % TEST.
