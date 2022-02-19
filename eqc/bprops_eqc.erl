%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
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
-module(bprops_eqc).

%%
%% This module defines a collection of EQC state_m commands, for
%% testing the riak_core_bucket module.  In order to understand this
%% test, you should understand EQC generally, and the EQC state machine
%% testing framework and callback conventions.
%%
%% TODO This module currently tests a limited subset of the
%%      riak_core_bucket module and makes little attempt to
%%      do negative testing around malformed inputs, etc.
%%      More attention needs to be spent on these tests!
%%

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-compile([export_all, nowarn_export_all]).

-type bucket_name() :: binary().
-type orddict() :: orddict:orddict().

-define(NAMES, [<<0>>, <<1>>, <<2>>, <<3>>]).
-define(BPROP_KEYS, [foo, bar, tapas]).
-define(DEFAULT_BPROPS, [{n_val, 3}]).

%%
%% The state_m "Model".  This invariant represents what properties
%% should be in which buckets between state transitions.
%%
-record(state, {
    buckets = orddict:new() :: orddict()
}).

%%
%% eqc_statem initial model
%%

-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    #state{}.

%%
%% set_bucket command
%%

set_bucket_args(_S) ->
    [bucket_name(), bucket_props()].

set_bucket(Bucket, BProps) ->
    riak_core_bucket:set_bucket(Bucket, BProps).

set_bucket_post(#state{buckets=Buckets}, [Bucket, _BProps], Res) ->
    case {Res, orddict:find(Bucket, Buckets)} of
        %% first time bucket has been set
        {ok, error} ->
            true;
        %% bucket has been set before
        {ok, {ok, _OldBProps}} ->
            true;
        %% anything other than ok is a failure
        %% TODO revisit, e.g., generate invalid inputs to force an error
        _ ->
            false
    end.

set_bucket_next(#state{buckets=Buckets} = S, _Res, [Bucket, BProps]) ->
    %%
    %% Get any previously defined properties from the model
    %%
    OldBProps =
        case orddict:find(Bucket, Buckets) of
            {ok, Props} -> Props;
            error -> orddict:from_list(?DEFAULT_BPROPS)
        end,
    S#state{
        buckets = orddict:store(
            Bucket,
            %% add defaults and the bucket name; remove any duplicates
            %% bprops takes precedence over defaults, and name is always set
            %% to bucket
            expected_properties(
                Bucket, OldBProps, BProps
            ),
            Buckets
        )
    }.

-spec expected_properties(bucket_name(), orddict(), orddict()) -> orddict().
expected_properties(Bucket, OldProps, NewProps) ->
    Props = riak_core_bucket_props:merge(NewProps, OldProps),
    orddict:store(name, Bucket, Props).

%%
%% get_bucket command
%%

get_bucket_args(_S) ->
    [bucket_name()].

get_bucket(Bucket) ->
    riak_core_bucket:get_bucket(Bucket).

get_bucket_post(#state{buckets=Buckets}, [Bucket], Res) ->
    BPropsFind = orddict:find(Bucket, Buckets),
    case {Res, BPropsFind} of
        {error, _} ->
            eq(Res, error);
        {_, {ok, BProps}} ->
            eq(
                orddict:from_list(Res),
                orddict:from_list(BProps)
            );
        {_, error} ->
            eq(
                orddict:from_list(Res),
                orddict:from_list(?DEFAULT_BPROPS ++ [{name, Bucket}])
            )
    end.

%%
%% all_n command
%%

all_n_args(_) -> [].

all_n() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_bucket:all_n(Ring).

all_n_post(#state{buckets=Buckets}, [], Res) ->
    AllNVals = orddict:fold(
        fun(_Bucket, BProps, Accum) ->
            {ok, NVal} = orddict:find(n_val, BProps),
            [NVal | Accum]
        end,
        [],
        Buckets
    ) ++ [proplists:get_value(n_val, ?DEFAULT_BPROPS)],
    eq(ordsets:from_list(Res), ordsets:from_list(AllNVals)).


%% TODO Add more commands here

%%
%% generators
%%

bucket_name() ->
    eqc_gen:elements(?NAMES).

bucket_props() ->
    eqc_gen:list(bucket_prop()).

bucket_prop() ->
    eqc_gen:oneof(
        [
            {n_val, pos_integer()},
            {bucket_prop_name(), bucket_prop_value()}
        ]
    ).

pos_integer() ->
    ?LET(N, eqc_gen:nat(), N + 1).

bucket_prop_name() ->
    eqc_gen:elements(?BPROP_KEYS).

bucket_prop_value() ->
    eqc_gen:bool().


%%
%% eqc properties
%%

prop_buckets() ->
    ?SETUP(
        fun setup_cleanup/0,
        ?FORALL(Cmds, eqc_statem:commands(?MODULE),
            aggregate(eqc_statem:command_names(Cmds),
                ?TRAPEXIT(
                    try
                        %%
                        %% setup
                        %%
                        os:cmd("rm -rf ./riak_core_bucket_eqc_meta"),
                        application:set_env(riak_core, claimant_tick, 4294967295),
                        application:set_env(riak_core, broadcast_lazy_timer, 4294967295),
                        application:set_env(riak_core, broadcast_exchange_timer, 4294967295),
                        application:set_env(riak_core, metadata_hashtree_timer, 4294967295),
                        application:set_env(riak_core, default_bucket_props, ?DEFAULT_BPROPS),
                        application:set_env(riak_core, cluster_name, "riak_core_bucket_eqc"),
                        stop_pid(riak_core_ring_events, whereis(riak_core_ring_events)),
                        stop_pid(riak_core_ring_manager, whereis(riak_core_ring_manager)),
                        {ok, RingEvents} = riak_core_ring_events:start_link(),
                        {ok, _RingMgr} = riak_core_ring_manager:start_link(test),
                        {ok, Claimant} = riak_core_claimant:start_link(),
                        {ok, MetaMgr} = riak_core_metadata_manager:start_link([{data_dir, "./riak_core_bucket_eqc_meta"}]),
                        {ok, Hashtree} = riak_core_metadata_hashtree:start_link("./riak_core_bucket_eqc_meta/trees"),
                        {ok, Broadcast} = riak_core_broadcast:start_link(),

                        {H, S, Res} = eqc_statem:run_commands(?MODULE, Cmds),

                        %%
                        %% shut down
                        %%
                        stop_pid(riak_core_broadcast, Broadcast),
                        stop_pid(riak_core_metadata_hashtree, Hashtree),
                        stop_pid(riak_core_metadata_manager, MetaMgr),
                        stop_pid(riak_core_claimant, Claimant),
                        riak_core_ring_manager:stop(),
                        stop_pid(riak_core_ring_events2, RingEvents),

                        eqc_statem:pretty_commands(
                            ?MODULE, Cmds,
                            {H, S, Res},
                            eqc:aggregate(
                                eqc_statem:command_names(Cmds),
                                Res == ok
                            )
                        )
                    after
                        os:cmd("rm -rf ./riak_core_bucket_eqc_meta")
                    end
                )
            )
        )
    ).

setup_cleanup() ->
    error_logger:tty(false),
    meck:new(riak_core_capability, []),
    meck:expect(
        riak_core_capability, get,
        fun({riak_core, bucket_types}) -> true;
            (X) -> meck:passthrough([X])
        end
    ),
    fun() ->
        error_logger:tty(true),
        meck:unload(riak_core_capability)
    end.

%%
%% internal helper functions
%%

stop_pid(_Tag, Other) when not is_pid(Other) ->
    ok;
stop_pid(Tag, Pid) ->
    unlink(Pid),
    exit(Pid, shutdown),
    ok = wait_for_pid(Tag, Pid).

wait_for_pid(Tag, Pid) ->
    Mref = erlang:monitor(process, Pid),
    receive
        {'DOWN', Mref, process, _, _} ->
            ok
    after
        5000 ->
            demonitor(Mref, [flush]),
	    exit(Pid, kill),
	    wait_for_pid(Tag, Pid)

    end.

-endif.
