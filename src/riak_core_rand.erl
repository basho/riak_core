%% Generalized random module that offers a backwards compatible API
%% around some of the changes in rand, crypto and for time units.

-module(riak_core_rand).

%% API
-export([
         uniform/0,
         uniform/1,
         uniform_s/2,
         seed/0,
         seed/1,
         rand_seed/0,
         rand_bytes/1
        ]).

%% As the algorithm is not changed in any place  we can use the default
%% algorithm for all call here.
-define(ALGO, exsplus).

%%%===================================================================
%%% New (r19+) rand style functions
%%%===================================================================
-ifdef(rand).
uniform() ->
    rand:uniform().

uniform(N) ->
    rand:uniform(N).

%% The old random:uniform_s took a 3 touple however this is no longer
%% the case, so what we need to do if we see such a situation is to first
%% create a state using seed_s (which can take the old data) and then
%% using uniform_s with this newly generated state.
%%
%% Note that seed_s does **not** change the current seed but just
%% create a new seed state.
uniform_s(N, {A, B, C}) ->
    State = rand:seed_s(?ALGO, {A, B, C}),
    rand:uniform_s(N, State);
uniform_s(N, State) ->
    rand:uniform_s(N, State).

seed() ->
    rand:seed(?ALGO).

%% We are a bit tricky here, while random:seed did return the **prior** seed
%% rand:seed will return the **new** seed. We can work around this by first
%% getting the exported seed then using this instead.
-spec seed({integer(),integer(),integer()} | rand:export_state()) ->
    rand:export_state() | undefined.
seed({_, _, _} = Seed) ->
    Old = rand:export_seed(),
    _New = rand:seed(?ALGO, Seed),
    Old;
seed(Seed) ->
    Old = rand:export_seed(),
    _New = rand:seed(Seed),
    Old.

rand_bytes(Size) ->
    crypto:strong_rand_bytes(Size).

-else.
%%%===================================================================
%%% Old (r18) random style functions
%%%===================================================================

uniform() ->
    random:uniform().

uniform(N) ->
    random:uniform(N).

uniform_s(N, State) ->
    random:uniform_s(N, State).

seed() ->
    random:seed().

seed({A, B, C}) ->
    random:seed({A, B, C}).

rand_bytes(Size) ->
    crypto:strong_rand(Size).

-endif.

%%%===================================================================
%%% General functions
%%%===================================================================

rand_seed() ->
    erlang:timestamp().
