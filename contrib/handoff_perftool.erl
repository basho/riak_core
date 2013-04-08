%%
%% Helper program for testing and measuring handoff performance.
%%
%% Usage example (in the Erlang shell):
%%
%% code:add_path("/home/user/path-to-handoff_perftool"), l(handoff_perftool). 
%% handoff_perftool:go({10000, 1000}).
%% handoff_perftool:go(5, {10000, 1000}).
%%

-module(handoff_perftool).
 
-export([
            go/0, go/1, go/2,

            force_encoding/2,
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


go() -> 
    go(1, {10000, 1000}).

go({NObjs, ValueSize}) ->
    go(1, {NObjs, ValueSize}).

go(NVnodes, {NObjs, ValueSize}) ->

    Secondaries = get_secondaries(),

    case length(Secondaries) >= NVnodes of
        false -> erlang:throw("Insufficent vnodes for requested test");
        true  -> true
    end,

    %% Select the requested number of secondaries from the whole list:
    TargetSecondaries = lists:sublist(get_secondaries(), NVnodes),

io:format("JFW: TargetSecondaries: ~p~n", [TargetSecondaries]),

    io:format("Seeding ~p objects of size ~p to ~p nodes...~n", [NObjs, ValueSize, NVnodes]),
    lists:map(fun(Secondary) -> seed_data({NObjs, ValueSize}, Secondary) end, TargetSecondaries),

    io:format("Done seeding, forcing handoff.~n"),
    riak_core_vnode_manager:force_handoffs(),

    io:format("Ok.~n"),

    true.

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

%%%%%%%%%%

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

%% particular handoff encoding method:
force_encoding(Node, HandoffEncoding) ->
    case HandoffEncoding of
        default -> lager:info("Using default encoding type."), true;

        _       -> lager:info("Forcing encoding type to ~p.", [HandoffEncoding]),
                   OverrideData =
                    [
                      { riak_core,
                            [
                                { override_capability,
                                        [
                                          { handoff_data_encoding,
                                                [
                                                  {    use, HandoffEncoding},
                                                  { prefer, HandoffEncoding}
                                                ]
                                          }
                                        ]
                                }
                            ]
                      }
                    ],

                   rt:update_app_config(Node, OverrideData)

    end,
    ok.

