-module(vclock_qc).

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

-define(ACTOR_IDS, [a,b,c,d,e]).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-record(state, {vclocks = []}).

-define(TEST_TIME, 20).

eqc_test_() ->
    {timeout,
     60,
     ?_assert(quickcheck(eqc:testing_time(?TEST_TIME, more_commands(10,?QC_OUT(prop_vclock())))))}.

test() ->
    quickcheck(eqc:testing_time(?TEST_TIME, more_commands(10, prop_vclock()))).

test(Time) ->
    quickcheck(eqc:testing_time(Time, more_commands(10, prop_vclock()))).


%% Initialize the state
initial_state() ->
   #state{}.

%% Command generator, S is the state
command(#state{vclocks=Vs}) ->
    oneof([{call, ?MODULE, fresh, []}] ++
          [{call, ?MODULE, timestamp, []}] ++
          [{call, ?MODULE, increment, [gen_actor_id(), elements(Vs)]} || length(Vs) > 0] ++
          [{call, ?MODULE, get_counter, [gen_actor_id(), elements(Vs)]} || length(Vs) > 0] ++
          [{call, ?MODULE, get_timestamp, [gen_actor_id(), elements(Vs)]} || length(Vs) > 0] ++
          [{call, ?MODULE, get_dot, [gen_actor_id(), elements(Vs)]}     || length(Vs) > 0] ++
          [{call, ?MODULE, merge, [list(elements(Vs))]} || length(Vs) > 0] ++
          [{call, ?MODULE, descends, [elements(Vs), elements(Vs)]} || length(Vs) > 0] ++
          [{call, ?MODULE, descends_dot, [elements(Vs), elements(Vs), gen_actor_id()]} || length(Vs) > 0] ++
          [{call, ?MODULE, dominates, [elements(Vs), elements(Vs)]} || length(Vs) > 0]
          ).

%% Next state transformation, S is the current state
next_state(S,_V,{call,_,get_counter,[_, _]}) ->
    S;
next_state(S,_V,{call,_,get_timestamp,[_, _]}) ->
    S;
next_state(S,_V,{call,_,get_dot,[_, _]}) ->
    S;
next_state(S,_V,{call,_,descends,[_, _]}) ->
    S;
next_state(S,_V,{call,_,descends_dot,[_, _, _]}) ->
    S;
next_state(S,_V,{call,_,dominates,[_, _]}) ->
    S;
next_state(S, _V, {call,_,timestamp,_}) ->
    S;
next_state(#state{vclocks=Vs}=S,V,{call,_,_,_}) ->
    S#state{vclocks=Vs ++ [V]}.

%% Precondition, checked before command is added to the command sequence
precondition(_S,{call,_,_,_}) ->
    true.

%% Postcondition, checked after command has been evaluated
%% OBS: S is the state before next_state(S,_,<command>)
postcondition(_S, {call, _, get_counter, _}, {MRes, Res}) ->
    MRes == Res;
postcondition(_S, {call, _, get_timestamp, _}, {MRes, Res}) ->
    timestamp_values_equal(MRes, Res);
postcondition(_S, {call, _, get_dot, _}, {{_Actor, 0, 0}, undefined}) ->
    true;
postcondition(_S, {call, _, get_dot, _}, {{Actor, Cnt, TS}, {ok, {Actor, {Cnt, TS}}}}) ->
    true;
postcondition(_S, {call, _, get_dot, _}, {_, _}) ->
    false;
postcondition(_S, {call, _, descends, _}, {MRes, Res}) ->
    MRes == Res;
postcondition(_S, {call, _, descends_dot, _}, {MRes, Res}) ->
    MRes == Res;
postcondition(_S, {call, _, dominates, _}, {MRes, Res}) ->
    MRes == Res;
postcondition(_S, {call, _, merge, [Items]}, {MRes, Res}) ->
    model_compare(MRes, Res) andalso
        lists:all(fun({_, V}) ->
                          vclock:descends(Res, V)
                  end, Items);
postcondition(_S, _C, {MRes, Res}) ->
    model_compare(MRes, Res);
postcondition(_S, _C, _Res) ->
    true.


prop_vclock() ->
    ?FORALL(Cmds,commands(?MODULE),
            begin
                put(timestamp, 1),
                {H,S,Res} = run_commands(?MODULE,Cmds),
                aggregate([ length(V) || {_,V} <- S#state.vclocks ],
                aggregate(command_names(Cmds),
                          collect({num_vclocks_div_10, length(S#state.vclocks) div 10},
                                  pretty_commands(?MODULE,Cmds, {H,S,Res}, Res == ok))))
            end).

gen_actor_id() ->
    elements(?ACTOR_IDS).

gen_entry(Actor, S) ->
    ?LET({_M, V}, elements(S), vclock:get_dot(Actor, V)).

fresh() ->
    {new_model(), vclock:fresh()}.

dominates({AM, AV}, {BM, BV}) ->
    {model_descends(AM, BM) andalso not model_descends(BM, AM),
     vclock:dominates(AV, BV)}.

descends({AM, AV}, {BM, BV}) ->
    {model_descends(AM, BM),
     vclock:descends(AV, BV)}.

descends_dot(A, {BM, BV}, Actor) ->
    {AMDot, AVDot} = get_dot(Actor, A),
    ModelDescends = model_descends_dot(BM, AMDot),
    case AVDot of
        undefined ->
            {ModelDescends, true};
        {ok, Dot} ->
            {ModelDescends, vclock:descends_dot(BV, Dot)}
    end.

get_counter(A, {M, V}) ->
    {model_counter(A, M), vclock:get_counter(A, V)}.

get_dot(A, {M, V}) ->
    {model_entry(A, M), vclock:get_dot(A, V)}.

increment(A, {M, V}) ->
    TS = timestamp(),
    MCount = model_counter(A, M),
    {lists:keyreplace(A, 1, M, {A, MCount + 1, TS}), vclock:increment(A, TS, V)}.

merge(List) ->
    {Models, VClocks} = lists:unzip(List),
    {lists:foldl(fun model_merge/2, new_model(), Models),
     vclock:merge(VClocks)}.

model_merge(M,OM) ->
    [ if C1 > C2 -> {A, C1, T1};
         C1 == C2 -> {A, C1, erlang:max(T1, T2)};
         true -> {A, C2, T2}
      end
      || {{A, C1, T1}, {A, C2, T2}} <- lists:zip(M, OM) ].

new_model() ->
    [{ID, 0, 0} || ID <- ?ACTOR_IDS].

model_compare(M, V) ->
    lists:all(fun(A) ->
                      model_counter(A,M) == vclock:get_counter(A,V) andalso
                          timestamps_equal(A, M, V)
              end,
              ?ACTOR_IDS).

model_descends(AM, BM) ->
   lists:all(fun({{Actor, Count1, _TS1}, {Actor, Count2, _TS2}}) ->
                     Count1 >= Count2
             end,
             lists:zip(AM, BM)).

model_descends_dot(M, {A, C, _TS}) ->
    {A, MC, _MTS} = lists:keyfind(A, 1, M),
    MC >= C.

get_timestamp(A, {M, V}) ->
    {model_timestamp(A,M), vclock:get_timestamp(A, V)}.

timestamp() ->
    put(timestamp, get(timestamp) + 1).

model_counter(A, M) ->
    element(2, lists:keyfind(A, 1, M)).

model_timestamp(A, M) ->
    element(3, lists:keyfind(A, 1, M)).

model_entry(A, M) ->
    lists:keyfind(A, 1, M).

timestamps_equal(A, M, V) ->
    VT = vclock:get_timestamp(A,V),
    MT = model_timestamp(A,M),
    timestamp_values_equal(MT, VT).

timestamp_values_equal(0, undefined) ->
    true;
timestamp_values_equal(T, T) ->
    true;
timestamp_values_equal(_, _) ->
    false.

-endif.
