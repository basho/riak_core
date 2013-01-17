-module(vclock_qc).

-ifdef(EQC).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

-define(ACTOR_IDS, [a,b,c,d,e]).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-record(state,{vclock, model}).

eqc_test_() ->
    ?_assert(eqc:quickcheck(?QC_OUT(prop_vclock()))).


%% Initialize the state
initial_state() ->
    #state{vclock=vclock:fresh(),
           model=new_model()
          }.

%% Command generator, S is the state
command(S) ->
    oneof([
           {call, vclock, increment, [gen_actor_id(), S#state.vclock]},
           {call, vclock, get_counter, [gen_actor_id(), S#state.vclock]},
           {call, ?MODULE, merge, [gen_vclock(), S#state.vclock]}
          ]).

%% Next state transformation, S is the current state
next_state(S,V,{call,vclock,increment,[ActorId, _]}) ->
    S#state{vclock=V,
            model=orddict:update_counter(ActorId, 1, S#state.model)};
next_state(S,_V,{call,vclock,get_counter,[_, _]}) ->
    S;
next_state(S,V,{call,_,merge,[{O,_}, _VClock]}) ->
    S#state{vclock=V, model=model_merge(S#state.model, O)};
next_state(S,_V,{call,_,_,_}) ->
    S.

%% Precondition, checked before command is added to the command sequence
precondition(_S,{call,_,_,_}) ->
    true.

%% Postcondition, checked after command has been evaluated
%% OBS: S is the state before next_state(S,_,<command>)
postcondition(#state{model=M},{call,vclock,increment,[ActorId, Vclock]},Res) ->
    %% Partial ordering property (descends)
    vclock:descends(Res,Vclock) andalso
    %% Counter has been incremented
        vclock:get_counter(ActorId, Res) == orddict:fetch(ActorId,M) + 1;
postcondition(#state{model=M}, {call,vclock,get_counter,[ActorId, _]}, Res) ->
    %% The counter for a given actor is the same as the model
    Res == orddict:fetch(ActorId, M);
postcondition(#state{model=M}, {call,_,merge,[{OM,Other},VClock]}, Res) ->
    Merged = model_merge(M, OM),
    %% The merged state has all the same actor counts
    lists:all(fun(A) -> orddict:fetch(A,Merged) == vclock:get_counter(A,Res) end, 
              ?ACTOR_IDS) andalso
        %% The merged vector clock descends from both inputs
        vclock:descends(Res, Other) andalso vclock:descends(Res, VClock);
postcondition(_S, _C, _Res) ->
    true.


prop_vclock() ->
    ?FORALL(Cmds,commands(?MODULE),
            begin
                {H,S,Res} = run_commands(?MODULE,Cmds),
                aggregate(command_names(Cmds),
                          pretty_commands(?MODULE,Cmds, {H,S,Res}, Res == ok))
            end).

gen_actor_id() ->
    elements(?ACTOR_IDS).

gen_vclock() ->
    frequency([
               {1, {new_model(), vclock:fresh()}},
               {3, ?LAZY(gen_incr_vclock())}
              ]).

gen_incr_vclock() ->
    ?LET({Actor, {M,Vc}},
         {gen_actor_id(), gen_vclock()},
         {orddict:update_counter(Actor, 1, M), vclock:increment(Actor, Vc)}).

merge({_,O}, V) ->
    vclock:merge([O,V]).

model_merge(M,OM) ->
    orddict:merge(fun(_K,A,B) -> erlang:max(A,B) end, M, OM).

new_model() ->
    orddict:from_list([{ID, 0} || ID <- ?ACTOR_IDS]).
-endif.
