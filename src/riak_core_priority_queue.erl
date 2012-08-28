%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

%% Priority queues have essentially the same interface as ordinary
%% queues, except that a) there is an in/3 that takes a priority, and
%% b) we have only implemented the core API we need.
%%
%% Priorities should be integers - the higher the value the higher the
%% priority - but we don't actually check that.
%%
%% in/2 inserts items with priority 0.
%%
%% We optimise the case where a priority queue is being used just like
%% an ordinary queue. When that is the case we represent the priority
%% queue as an ordinary queue. We could just call into the 'queue'
%% module for that, but for efficiency we implement the relevant
%% functions directly in here, thus saving on inter-module calls and
%% eliminating a level of boxing.
%%
%% When the queue contains items with non-zero priorities, it is
%% represented as a sorted kv list with the inverted Priority as the
%% key and an ordinary queue as the value. Here again we use our own
%% ordinary queue implemention for efficiency, often making recursive
%% calls into the same function knowing that ordinary queues represent
%% a base case.


-module(riak_core_priority_queue).

-export([new/0, is_queue/1, is_empty/1, len/1, to_list/1, in/2, in/3,
         out/1, out/2, pout/1, join/2]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(priority() :: integer()).
-type(squeue() :: {queue, [any()], [any()]}).
-type(pqueue() ::  squeue() | {pqueue, [{priority(), squeue()}]}).

-spec(new/0 :: () -> pqueue()).
-spec(is_queue/1 :: (any()) -> bool()).
-spec(is_empty/1 :: (pqueue()) -> bool()).
-spec(len/1 :: (pqueue()) -> non_neg_integer()).
-spec(to_list/1 :: (pqueue()) -> [{priority(), any()}]).
-spec(in/2 :: (any(), pqueue()) -> pqueue()).
-spec(in/3 :: (any(), priority(), pqueue()) -> pqueue()).
-spec(out/1 :: (pqueue()) -> {(empty | {value, any()}), pqueue()}).
-spec(out/2 :: (priority(), pqueue()) -> {(empty | {value, any()}), pqueue()}).
-spec(pout/1 :: (pqueue()) -> {(empty | {value, any(), priority()}), pqueue()}).
-spec(join/2 :: (pqueue(), pqueue()) -> pqueue()).

-endif.

%%----------------------------------------------------------------------------

new() ->
    {queue, [], []}.

is_queue({queue, R, F}) when is_list(R), is_list(F) ->
    true;
is_queue({pqueue, Queues}) when is_list(Queues) ->
    lists:all(fun ({P, Q}) -> is_integer(P) andalso is_queue(Q) end,
              Queues);
is_queue(_) ->
    false.

is_empty({queue, [], []}) ->
    true;
is_empty(_) ->
    false.

len({queue, R, F}) when is_list(R), is_list(F) ->
    length(R) + length(F);
len({pqueue, Queues}) ->
    lists:sum([len(Q) || {_, Q} <- Queues]).

to_list({queue, In, Out}) when is_list(In), is_list(Out) ->
    [{0, V} || V <- Out ++ lists:reverse(In, [])];
to_list({pqueue, Queues}) ->
    [{-P, V} || {P, Q} <- Queues, {0, V} <- to_list(Q)].

in(Item, Q) ->
    in(Item, 0, Q).

in(X, 0, {queue, [_] = In, []}) ->
    {queue, [X], In};
in(X, 0, {queue, In, Out}) when is_list(In), is_list(Out) ->
    {queue, [X|In], Out};
in(X, Priority, _Q = {queue, [], []}) ->
    in(X, Priority, {pqueue, []});
in(X, Priority, Q = {queue, _, _}) ->
    in(X, Priority, {pqueue, [{0, Q}]});
in(X, Priority, {pqueue, Queues}) ->
    P = -Priority,
    {pqueue, case lists:keysearch(P, 1, Queues) of
                 {value, {_, Q}} ->
                     lists:keyreplace(P, 1, Queues, {P, in(X, Q)});
                 false ->
                     lists:keysort(1, [{P, {queue, [X], []}} | Queues])
             end}.

out({queue, [], []} = Q) ->
    {empty, Q};
out({queue, [V], []}) ->
    {{value, V}, {queue, [], []}};
out({queue, [Y|In], []}) ->
    [V|Out] = lists:reverse(In, []),
    {{value, V}, {queue, [Y], Out}};
out({queue, In, [V]}) when is_list(In) ->
    {{value,V}, r2f(In)};
out({queue, In,[V|Out]}) when is_list(In) ->
    {{value, V}, {queue, In, Out}};
out({pqueue, [{P, Q} | Queues]}) ->
    {R, Q1} = out(Q),
    NewQ = case is_empty(Q1) of
               true -> case Queues of
                           []           -> {queue, [], []};
                           [{0, OnlyQ}] -> OnlyQ;
                           [_|_]        -> {pqueue, Queues}
                       end;
               false -> {pqueue, [{P, Q1} | Queues]}
           end,
    {R, NewQ}.

out(_Priority, {queue, [], []} = Q) ->
    {empty, Q};
out(Priority, {queue, _, _} = Q) when Priority =< 0 ->
    out(Q);
out(_Priority, {queue, _, _} = Q) ->
    {empty, Q};
out(Priority, {pqueue, [{P, _Q} | _Queues]} = Q) when Priority =< (-P) ->
    out(Q);
out(_Priority, {pqueue, [_|_]} = Q) ->
    {empty, Q}.

pout({queue, [], []} = Q) ->
    {empty, Q};
pout({queue, _, _} = Q) ->
    {{value, V}, Q1} = out(Q),
    {{value, V, 0}, Q1};
pout({pqueue, [{P, Q} | Queues]}) ->
    {{value, V}, Q1} = out(Q),
    NewQ = case is_empty(Q1) of
               true -> case Queues of
                           []           -> {queue, [], []};
                           [{0, OnlyQ}] -> OnlyQ;
                           [_|_]        -> {pqueue, Queues}
                       end;
               false -> {pqueue, [{P, Q1} | Queues]}
           end,
    {{value, V, -P}, NewQ}.

join(A, {queue, [], []}) ->
    A;
join({queue, [], []}, B) ->
    B;
join({queue, AIn, AOut}, {queue, BIn, BOut}) ->
    {queue, BIn, AOut ++ lists:reverse(AIn, BOut)};
join(A = {queue, _, _}, {pqueue, BPQ}) ->
    {Pre, Post} = lists:splitwith(fun ({P, _}) -> P < 0 end, BPQ),
    Post1 = case Post of
                []                        -> [ {0, A} ];
                [ {0, ZeroQueue} | Rest ] -> [ {0, join(A, ZeroQueue)} | Rest ];
                _                         -> [ {0, A} | Post ]
            end,
    {pqueue, Pre ++ Post1};
join({pqueue, APQ}, B = {queue, _, _}) ->
    {Pre, Post} = lists:splitwith(fun ({P, _}) -> P < 0 end, APQ),
    Post1 = case Post of
                []                        -> [ {0, B} ];
                [ {0, ZeroQueue} | Rest ] -> [ {0, join(ZeroQueue, B)} | Rest ];
                _                         -> [ {0, B} | Post ]
            end,
    {pqueue, Pre ++ Post1};
join({pqueue, APQ}, {pqueue, BPQ}) ->
    {pqueue, merge(APQ, BPQ, [])}.

merge([], BPQ, Acc) ->
    lists:reverse(Acc, BPQ);
merge(APQ, [], Acc) ->
    lists:reverse(Acc, APQ);
merge([{P, A}|As], [{P, B}|Bs], Acc) ->
    merge(As, Bs, [ {P, join(A, B)} | Acc ]);
merge([{PA, A}|As], Bs = [{PB, _}|_], Acc) when PA < PB ->
    merge(As, Bs, [ {PA, A} | Acc ]);
merge(As = [{_, _}|_], [{PB, B}|Bs], Acc) ->
    merge(As, Bs, [ {PB, B} | Acc ]).

r2f([])      -> {queue, [], []};
r2f([_] = R) -> {queue, [], R};
r2f([X,Y])   -> {queue, [X], [Y]};
r2f([X,Y|R]) -> {queue, [X,Y], lists:reverse(R, [])}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

simple_case(Order) ->
    Queue = ?MODULE:new(),
    ?assertEqual(true, ?MODULE:is_queue(Queue)),
    ?assertEqual(true, ?MODULE:is_empty(Queue)),
    ?assertEqual(0, ?MODULE:len(Queue)),
    ?assertEqual([], ?MODULE:to_list(Queue)),
    case Order of
        forward ->
            Queue2 = ?MODULE:in(low, Queue),
            Queue3 = ?MODULE:in(mid, 500, Queue2),
            Queue4 = ?MODULE:in(high, 1000, Queue3);
        reverse ->
            Queue2 = ?MODULE:in(high, 1000, Queue),
            Queue3 = ?MODULE:in(mid, 500, Queue2),
            Queue4 = ?MODULE:in(low, Queue3);
        mixed ->
            Queue2 = ?MODULE:in(high, 1000, Queue),
            Queue3 = ?MODULE:in(low, Queue2),
            Queue4 = ?MODULE:in(mid, 500, Queue3)
    end,
    ?assertEqual(false, ?MODULE:is_empty(Queue4)),
    ?assertEqual(3, ?MODULE:len(Queue4)),
    ?assertMatch({{value, high}, _}, ?MODULE:out(Queue4)),
    {{value, high}, Queue5} = ?MODULE:out(Queue4),
    ?assertMatch({{value, mid}, _}, ?MODULE:out(Queue5)),
    {{value, mid}, Queue6} = ?MODULE:out(Queue5),
    ?assertMatch({{value, low}, _}, ?MODULE:out(Queue6)),
    {{value, low}, Queue7} = ?MODULE:out(Queue6),
    ?assertEqual(0, ?MODULE:len(Queue7)),

    ?assertEqual(true, ?MODULE:is_queue(Queue2)),
    ?assertEqual(true, ?MODULE:is_queue(Queue3)),
    ?assertEqual(true, ?MODULE:is_queue(Queue4)),
    ?assertEqual(false, ?MODULE:is_queue([])),
    ok.

merge_case() ->
    QueueA1 = ?MODULE:new(),
    QueueA2 = ?MODULE:in(1, QueueA1),
    QueueA3 = ?MODULE:in(3, QueueA2),
    QueueA4 = ?MODULE:in(5, QueueA3),

    QueueB1 = ?MODULE:new(),
    QueueB2 = ?MODULE:in(2, QueueB1),
    QueueB3 = ?MODULE:in(4, QueueB2),
    QueueB4 = ?MODULE:in(6, QueueB3),

    Merged1 = ?MODULE:join(QueueA4, QueueB4),
    ?assertEqual([{0,1},{0,3},{0,5},{0,2},{0,4},{0,6}],
                 ?MODULE:to_list(Merged1)),

    QueueC1 = ?MODULE:new(),
    QueueC2 = ?MODULE:in(1, 10, QueueC1),
    QueueC3 = ?MODULE:in(3, 30, QueueC2),
    QueueC4 = ?MODULE:in(5, 50, QueueC3),

    QueueD1 = ?MODULE:new(),
    QueueD2 = ?MODULE:in(2, 20, QueueD1),
    QueueD3 = ?MODULE:in(4, 40, QueueD2),
    QueueD4 = ?MODULE:in(6, 60, QueueD3),

    Merged2 = ?MODULE:join(QueueC4, QueueD4),
    ?assertEqual([{60,6},{50,5},{40,4},{30,3},{20,2},{10,1}],
                 ?MODULE:to_list(Merged2)),
    ok.

basic_test() ->
    simple_case(forward),
    simple_case(reverse),
    simple_case(mixed),
    merge_case(),
    ok.

-endif.
