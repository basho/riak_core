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

eqc_test_() ->
    ?_assert(eqc:quickcheck(?QC_OUT(prop_vclock()))).


%% Initialize the state
initial_state() ->
   #state{}.

%% Command generator, S is the state
command(#state{vclocks=Vs}) ->
    oneof([{call, ?MODULE, fresh, []}] ++
          [{call, ?MODULE, increment, [gen_actor_id(), elements(Vs)]} || length(Vs) > 0] ++
          [{call, ?MODULE, get_counter, [gen_actor_id(), elements(Vs)]} || length(Vs) > 0] ++
          [{call, ?MODULE, merge, [list(elements(Vs))]} || length(Vs) > 0] ++
          [{call, ?MODULE, descends, [elements(Vs), elements(Vs)]} || length(Vs) > 0] ++
          [{call, ?MODULE, dominates, [elements(Vs), elements(Vs)]} || length(Vs) > 0]
          ).

%% Next state transformation, S is the current state
next_state(S,_V,{call,_,get_counter,[_, _]}) ->
    S;
next_state(S,_V,{call,_,descends,[_, _]}) ->
    S;
next_state(S,_V,{call,_,dominates,[_, _]}) ->
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
postcondition(_S, {call, _, descends, _}, {MRes, Res}) ->
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
                {H,S,Res} = run_commands(?MODULE,Cmds),
                aggregate(command_names(Cmds),
                          collect({num_vclocks, length(S#state.vclocks) div 10},
                                  pretty_commands(?MODULE,Cmds, {H,S,Res}, Res == ok)))
            end).

gen_actor_id() ->
    elements(?ACTOR_IDS).

gen_vclock() ->
    ?SIZED(Size, gen_vclock(Size)).

gen_vclock(Size) ->
    ?LAZY(frequency([{1, {new_model(), vclock:fresh()}} ] ++
              [ {3, gen_incr_vclock(Size - 1)} || Size > 0] ++
              [ {1, gen_merged_vclock(Size div 2)} || Size > 0 ])).

gen_merged_vclock(Size) ->
    ?LETSHRINK([{AM, AV}, {BM, BV}],
               [gen_vclock(Size), gen_vclock(Size)],
               {model_merge(AM, BM), vclock:merge([AV, BV])}).

gen_incr_vclock(Size) ->
    ?LET({Actor, {M,Vc}},
         {gen_actor_id(), gen_vclock(Size)},
         ?SHRINK({orddict:update_counter(Actor, 1, M), vclock:increment(Actor, Vc)}, [{M,Vc}])).

fresh() ->
    {new_model(), vclock:fresh()}.

dominates({AM, AV}, {BM, BV}) ->
    {model_descends(AM, BM) andalso AM =/= BM,
     vclock:dominates(AV, BV)}.

descends({AM, AV}, {BM, BV}) ->
    {model_descends(AM, BM),
     vclock:descends(AV, BV)}.

get_counter(A, {M, V}) ->
    {orddict:fetch(A, M), vclock:get_counter(A, V)}.

increment(A, {M, V}) ->
    {orddict:update_counter(A, 1, M), vclock:increment(A, V)}.

merge(List) ->
    {Models, VClocks} = lists:unzip(List),
    {lists:foldl(fun model_merge/2, new_model(), Models),
     vclock:merge(VClocks)}.

model_merge(M,OM) ->
    orddict:merge(fun(_K,A,B) -> erlang:max(A,B) end, M, OM).

new_model() ->
    orddict:from_list([{ID, 0} || ID <- ?ACTOR_IDS]).

model_compare(M, V) ->
    lists:all(fun(A) ->
                      orddict:fetch(A,M) == vclock:get_counter(A,V)
              end,
              ?ACTOR_IDS).

model_descends(AM, BM) ->
   lists:all(fun({Actor, Count}) ->
                     Count >= orddict:fetch(Actor, BM)
             end,
             AM).

-endif.
