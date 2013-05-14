%%
%% Helper program for testing and measuring handoff performance.
%%
%% Usage example (in the Erlang shell):
%%
%% code:add_path("/home/user/path-to-handoff_perftool"), l(handoff_perftool). 
%% handoff_perftool:go({10000, 1000}).                          % use 10000 objects of 1000 bytes each
%% handoff_perftool:go(5, {10000, 1000}).                       % use 5 vnodes and 10000 objects of 1000 bytes each
%% handoff_perftool:go(1, {10000, 1000}, strategy_roundrobin).  % 1 vnode, 10k objects of 1k each, round-robin strategy
%%

-module(handoff_perftool).
 
-export([
            %% If we get any more options, we should use something associative rather than adding more options:
            go/0, go/1, go/2, go/3,

            get_ring_members/0,
            get_ring_owners/0
        ]).
 
-include("riak_core_vnode.hrl").
-include("riak_core_handoff.hrl").

-ifndef(MD_VTAG).
 -define(MD_VTAG,     <<"X-Riak-VTag">>).
-endif.
-ifndef(MD_LASTMOD).
 -define(MD_LASTMOD,  <<"X-Riak-Last-Modified">>).
-endif.

-define(HARNESS, (rt:config(rt_harness))).

%% JFW: hack until we can get this to play nicely with rebar:
log_info(Message) -> lager:log(info, self(), Message).
log_info(Message, Params) -> lager:log(info, self(), Message, Params).

go() -> 
    go(1, {10000, 1000}).

go({NObjs, ValueSize}) ->
    go(1, {NObjs, ValueSize}).

go(NVnodes, {NObjs, ValueSize}) ->
    go(NVnodes, { NObjs, ValueSize }, strategy_other_owner).

go(NVnodes, {NObjs, ValueSize}, GatherStrategy) ->
    go(NVnodes, {NObjs, ValueSize}, GatherStrategy, use_existing_concurrency).

go(NVnodes, {NObjs, ValueSize}, GatherStrategy, ConcurrencyN) ->

    Targets = gather_targets(NVnodes, GatherStrategy),

    log_info("Seeding ~p objects of size ~p to ~p nodes by strategy ~p...~n", [NObjs, ValueSize, NVnodes, GatherStrategy]),
    lists:map(fun(Target) -> seed_data({NObjs, ValueSize}, Target) end, Targets),
    log_info("Done seeding.~n"),

    OldConcurrencyN = set_handoff_concurrency(ConcurrencyN),

    log_info("Forcing handoff.~n"),
    riak_core_vnode_manager:force_handoffs(),
    log_info("Done forcing handoff.~n"),

    %% Be a friendly citizen and restore the original concurrency settings:
    set_handoff_concurrency(OldConcurrencyN),

    true.

%%%%%%%%%%

%%
%% Different ways of gathering target vnodes for handoff:
%%

gather_targets(NVnodes, GatherStrategy) ->
    log_info("Using gather strategy ~p.~n", [GatherStrategy]),
    case GatherStrategy of 
        strategy_other_owner -> gather_vnodes_1(NVnodes);
        strategy_roundrobin  -> gather_vnodes_rr(NVnodes);
        _ -> log_info("Invalid gather strategy " ++ GatherStrategy),
             erlang:throw(invalid_gather_strategy)
    end.

%% Construct a list of target vnodes such that we select from vnodes that we don't own:
gather_vnodes_1(NVnodes) ->
    Secondaries = get_secondaries(),

    case length(Secondaries) >= NVnodes of
        false -> log_info("Insufficent vnodes for requested test (have ~p secondaries, require ~p)", [length(Secondaries), NVnodes]),
                 erlang:throw(insufficient_vnodes);
        true  -> true
    end,

    %% Select the requested number of secondaries from the whole list:
    lists:sublist(get_secondaries(), NVnodes).

%% Construct a list of target vnodes such that we select a total of N vnodes from different nodes,
%% round-robin fashion.
%%      Note: This algorithm is surely inefficient, but N is expected to be small.
gather_vnodes_rr(NVnodes) ->

    %% Map owners to their vnode ids (not including ourselves):
    HandoffMap = dict:erase(node(), lists:foldl(fun({NodeID, NodeName}, AccDict) ->
                                                    dict:append(NodeName, NodeID, AccDict)
                                                end, 
                                                dict:new(), get_ring_owners())),

    HandoffMembers = lists:dropwhile(fun(NodeName) -> node() == NodeName end, get_ring_members()),

    %% Find the smallest set in the group:
    {_MinKey, MinLen} = dict:fold(fun shortest_bucket/3, { undef, infinity }, HandoffMap),

    case MinLen < NVnodes of
        false -> ok;
        true  -> log_info("Requested more vnodes than available in smallest target"),
                 erlang:throw(requested_too_many_vnodes)
    end,

    merge_values(MinLen, HandoffMembers, HandoffMap, []).

%%
%% Selection utilities:
%%

%% Select secondary handoff vnodes (ones we don't own):
get_secondaries() ->
    get_secondaries(get_ring_owners(), node()).
get_secondaries(RingOwners, Node) ->
    [Index || {Index, RingOwner} <- RingOwners, RingOwner =/= Node].

get_ring_members() ->
    { ok, Ring } = riak_core_ring_manager:get_raw_ring(),
    riak_core_ring:all_members(Ring).

get_ring_owners() ->
    { ok, Ring } = riak_core_ring_manager:get_raw_ring(),
    riak_core_ring:all_owners(Ring).

%%
%% Data object utilities:
%%

%% Construct test handoff objects and send them to the requested vnode:
seed_data({0, _Size}, _SecondarySHA1) ->
    ok;
seed_data({NEntries, Size}, SecondarySHA1) ->

    RObj = finalize_object(riak_object:new(<<"test_bucket">>,
                                           <<NEntries:64/integer>>,
                                           %% <<NEntries:64/integer>>)),
                                           random_binary(Size, <<>>))),

    riak_kv_vnode:local_put(SecondarySHA1, RObj),
 
    seed_data({NEntries - 1, Size}, SecondarySHA1).

%% Construct a random binary object: 
random_binary(0, Bin) ->
    Bin;
random_binary(N, Bin) ->
    X = random:uniform(255),
    random_binary(N-1, <<Bin/binary, X:8/integer>>).

%% Directly "inject" a object w/ metadata, vtags, etc.: 
finalize_object(RObj) ->
    MD0 = case dict:find(clean, riak_object:get_update_metadata(RObj)) of
              {ok, true} ->
                  %% There have been no changes to updatemetadata. If we stash the
                  %% last modified in this dict, it will cause us to lose existing
                  %% metadata (bz://508). If there is only one instance of metadata,
                  %% we can safely update that one, but in the case of multiple siblings,
                  %% it's hard to know which one to use. In that situation, use the update
                  %% metadata as is.
                  case riak_object:get_metadatas(RObj) of
                      [MD] ->
                          MD;
                      _ ->
                          riak_object:get_update_metadata(RObj)
                  end;
               _ ->
                  riak_object:get_update_metadata(RObj)
          end,
    Now = os:timestamp(),
    NewMD = dict:store(?MD_VTAG, make_vtag(Now),
                       dict:store(?MD_LASTMOD, Now, MD0)),
    riak_object:apply_updates(riak_object:update_metadata(RObj, NewMD)).
 
make_vtag(Now) ->
    <<HashAsNum:128/integer>> = crypto:md5(term_to_binary({node(), Now})),
    riak_core_util:integer_to_list(HashAsNum,62).

%%
%% Other handoff-helper functions:
%%

%% Fiddle with the cluster's handoff concurrency:
set_handoff_concurrency(ConcurrencyN) when is_integer(ConcurrencyN) ->
    OriginalConcurrencyN = get_handoff_concurrency(),
    log_info("Prior concurrency setting ~p, setting to ~p.~n", [OriginalConcurrencyN, ConcurrencyN]),
    rpc:multicall(riak_core_handoff_manager, set_concurrency, [ConcurrencyN]),
    log_info("Done setting concurrency.~n"),
    OriginalConcurrencyN;

set_handoff_concurrency(ConcurrencySettings) when is_list(ConcurrencySettings) ->
    log_info("Restoring concurrency settings to ~p.~n", [ConcurrencySettings]),
    rpc:multicall(riak_core_handoff_manager, set_concurrency, ConcurrencySettings),
    log_info("Done restoring concurrency settings.~n");

set_handoff_concurrency(use_existing_concurrency) ->
    use_existing_concurrency.

get_handoff_concurrency() ->
    rpc:multicall(riak_core_handoff_manager, get_concurrency, []).

%%
%% General helper functions:
%%

%% N-way merge:
merge_values(0, _SourceKeys, _SourceMap, Acc) ->
    Acc;

merge_values(N, SourceKeys, SourceMap, Acc) ->
    { OutputAcc, OutputSourceMap } = 
        lists:foldl(fun(Key, { InnerAcc, InnerSourceMap }) ->
                        { Value, NewSourceMap } = pop_value(Key, InnerSourceMap),
                        { lists:append(InnerAcc, Value), NewSourceMap }
                    end,
                    { Acc, SourceMap },
                    SourceKeys),
    merge_values(N - 1, SourceKeys, OutputSourceMap, OutputAcc).

%% Collect the first value for a given key, then return the value and the mutated map:
pop_value(Key, SourceMap) ->
    Values = dict:fetch(Key, SourceMap),
    { Value, NewValues } = lists:split(1, Values),
    NewSourceMap = dict:store(Key, NewValues, SourceMap),
    { Value, NewSourceMap }.

%% Find the key and shortest length of "buckets" in a map of lists:
shortest_bucket(Key, ValueList, { _, infinity}) -> 
    { Key, length(ValueList) };
shortest_bucket(Key, ValueList, { MinKey, MinLen }) ->
    L = length(ValueList),
    case L < MinLen of
        false -> { MinKey, MinLen };
        true  -> { Key, L } 
    end.
