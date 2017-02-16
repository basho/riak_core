%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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
-module(bucket_fixup_test).

-include_lib("eunit/include/eunit.hrl").

-export([fixup/2]).

fixup(test1, Props) ->
    {ok, [{test, 1}|Props]};
fixup(test2, Props) ->
    case proplists:get_value(expand, Props) of
        undefined ->
            {ok, Props};
        X ->
            CleanProps = proplists:delete(expand, Props),
            {ok, [{expanded1, X}, {expanded2, X} | CleanProps]}
    end;
fixup(test3, Props) ->
    {ok, proplists:delete(suppress, Props)};
fixup(test4, _Props) ->
    {error, my_hovercraft_is_full_of_eels};
fixup(test5, Props) ->
    {ok, undef:this_is_undefined(Props)};
fixup(_, Props) ->
    {ok, Props}.

fixup_test_() ->
    {setup,
     fun() ->
             application:set_env(riak_core,ring_creation_size, 4),
             application:set_env(riak_core, bucket_fixups, [{someapp, ?MODULE}]),
             application:set_env(riak_core, default_bucket_props, [])
     end,
     fun(_) ->
             application:unset_env(riak_core, ring_creation_size),
             application:unset_env(riak_core, bucket_fixups),
             application:unset_env(riak_core, default_bucket_props),
             process_flag(trap_exit, true),
             catch application:stop(riak_core),
             riak_core_test_util:stop_pid(whereis(riak_core_ring_manager)),
             riak_core_test_util:stop_pid(whereis(riak_core_ring_events))
     end,
     [
      fun do_no_harm/0,
      fun property_addition/0,
      fun property_expansion/0,
      fun property_supression/0,
      fun fixup_error/0,
      fun fixup_crash/0
     ]
    }.

load_test_() ->
    {setup,
     fun() ->
             application:load(riak_core),
             application:set_env(riak_core, bucket_fixups, []),
             application:set_env(riak_core, default_bucket_props, []),
             application:set_env(riak_core, ring_creation_size, 64),
             riak_core_ring_events:start_link(),
             riak_core_ring_manager:start_link(test),
             ok
     end,
     fun(_) ->
             process_flag(trap_exit, true),
             catch application:stop(riak_core),
             riak_core_test_util:unlink_named_process(riak_core_ring_manager),
             catch(riak_core_ring_manager:stop()),
             riak_core_test_util:stop_pid(whereis(riak_core_ring_events), shutdown),
             application:unset_env(riak_core, bucket_fixups),
             application:unset_env(riak_core, default_bucket_props),
             application:unset_env(riak_core, ring_creation_size)
     end,
     [
      ?_test(begin
                 {ok, _R} = riak_core_ring_manager:get_my_ring(),
                 riak_core_bucket:set_bucket(test1, []),
                 ?assertEqual([{name, test1}],
                              riak_core_bucket:get_bucket(test1)),
                 riak_core:register(loadtestapp, [{bucket_fixup, ?MODULE}]),
                 ?assertEqual([{test, 1}, {name, test1}],
                              riak_core_bucket:get_bucket(test1)),
                 ok
             end)
     ]
    }.

do_no_harm() ->
    Ring = riak_core_ring:update_meta({bucket,test0},
                                      [],
                                      riak_core_ring:fresh()),
    riak_core_ring_manager:setup_ets(test),
    riak_core_ring_manager:set_ring_global(Ring),
    ?assertEqual([test0], riak_core_ring:get_buckets(Ring)),
    [ ?assertEqual([], riak_core_bucket:get_bucket(Bucket)) ||
        Bucket <- riak_core_ring:get_buckets(Ring)],
    ok.

property_addition() ->
    Ring = riak_core_ring:update_meta({bucket,test1},
                                      [{foo, bar}],
                                      riak_core_ring:fresh()),
    riak_core_ring_manager:setup_ets(test),
    riak_core_ring_manager:set_ring_global(Ring),
    ?assertEqual([test1], riak_core_ring:get_buckets(Ring)),
    [ ?assertEqual(1, proplists:get_value(test,
                                          riak_core_bucket:get_bucket(Bucket))) ||
        Bucket <- riak_core_ring:get_buckets(Ring)],
    ok.

property_expansion() ->
    Ref = foobar,
    Ring = riak_core_ring:update_meta({bucket,test2},
                                      [{expand, Ref}],
                                      riak_core_ring:fresh()),
    riak_core_ring_manager:setup_ets(test),
    riak_core_ring_manager:set_ring_global(Ring),
    ?assertEqual([test2], riak_core_ring:get_buckets(Ring)),
    ?assertMatch([{expanded1, Ref}, {expanded2, Ref}],
                 riak_core_bucket:get_bucket(test2)),
    ok.

property_supression() ->
    Ring = riak_core_ring:update_meta({bucket,test3},
                                      [{suppress, 1}, {foo, bar}],
                                      riak_core_ring:fresh()),
    riak_core_ring_manager:setup_ets(test),
    riak_core_ring_manager:set_ring_global(Ring),
    ?assertEqual([test3], riak_core_ring:get_buckets(Ring)),
    ?assertMatch([{foo, bar}],
                 riak_core_bucket:get_bucket(test3)),
    ok.

fixup_error() ->
    Ring = riak_core_ring:update_meta({bucket,test4},
                                      [{foo, bar}],
                                      riak_core_ring:fresh()),
    riak_core_ring_manager:setup_ets(test),
    riak_core_ring_manager:set_ring_global(Ring),
    ?assertEqual([test4], riak_core_ring:get_buckets(Ring)),
    ?assertMatch([{foo, bar}],
                 riak_core_bucket:get_bucket(test4)),
    ok.

fixup_crash() ->
    Ring = riak_core_ring:update_meta({bucket,test5},
                                      [{foo, bar}],
                                      riak_core_ring:fresh()),
    riak_core_ring_manager:setup_ets(test),
    riak_core_ring_manager:set_ring_global(Ring),
    ?assertEqual([test5], riak_core_ring:get_buckets(Ring)),
    ?assertMatch([{foo, bar}],
                 riak_core_bucket:get_bucket(test5)),
    ok.
