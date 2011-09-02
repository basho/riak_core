 %% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
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

%% @doc A module to provide access to riak_core configuration information.
%% @type riak_core_bucketprops() = [{Propkey :: atom(), Propval :: term()}]

-module(riak_core_config).

-author('Kelly McLaughlin <kelly@basho.com>').

-export([http_ip_and_port/0, ring_state_dir/0, ring_creation_size/0,
         default_bucket_props/0, cluster_name/0, gossip_interval/0,
         target_n_val/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% ===================================================================
%% Public API
%% ===================================================================

%% @spec http_ip_and_port() -> {string(), integer()} | error
%% @doc Get the HTTP IP address and port number environment variables.
http_ip_and_port() ->
    case get_riak_core_env(http) of
        [{WebIp, WebPort} | _] ->            
            {WebIp, WebPort};
        [] ->
            error;
        undefined ->
            %% Fallback to pre-0.14 HTTP config.                                                                                                                        
            %% TODO: Remove in 0.16                                                                                                                                     
            WebIp = get_riak_core_env(web_ip),
            WebPort = get_riak_core_env(web_port),
            if
                WebIp == undefined ->
                    error;
                WebPort == undefined ->
                    error;
                true ->
                    lager:warning("Found HTTP config for riak_core using pre-0.14 config "
                        "values; please update the config file to use new HTTP "
                        "binding configuration values."),
                    {WebIp, WebPort}
            end
    end.

%% @spec ring_state_dir() -> string() | undefined
%% @doc Get the ring_state_dir environment variable.
ring_state_dir() ->
    get_riak_core_env(ring_state_dir).

%% @spec ring_creation_size() -> integer() | undefined
%% @doc Get the ring_creation_size environment variable.
ring_creation_size() ->
    get_riak_core_env(ring_creation_size).  

%% @spec cluster_name() -> string() | undefined
%% @doc Get the cluster_name environment variable.
cluster_name() ->
    get_riak_core_env(cluster_name).

%% @spec gossip_interval() -> integer() | undefined
%% @doc Get the gossip_interval environment variable.
gossip_interval() ->
    get_riak_core_env(gossip_interval).

%% @spec target_n_val() -> integer() | undefined
%% @doc Get the target_n_val environment variable.
target_n_val() ->
    get_riak_core_env(target_n_val).

%% @spec default_bucket_props() -> BucketProps::riak_core_bucketprops() | undefined
%% @doc Get the default_bucket_props environment variable.
default_bucket_props() ->
    get_riak_core_env(default_bucket_props).

%% @private
get_riak_core_env(Key) ->
   app_helper:get_env(riak_core, Key).


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

riak_core_config_test_() ->
    { setup,
      fun setup/0,
      fun cleanup/1,
      [
       fun http_ip_and_port_test_case/0,
       fun default_bucket_props_test_case/0,
       fun target_n_val_test_case/0,
       fun gossip_interval_test_case/0,
       fun cluster_name_test_case/0,
       fun ring_creation_size_test_case/0,
       fun ring_state_dir_test_case/0,
       fun non_existent_var_test_case/0
      ]
    }.

http_ip_and_port_test_case() ->
    ?assertEqual(error, http_ip_and_port()),
    %% Test the pre-0.14 style config
    application:set_env(riak_core, web_ip, "127.0.0.1"),
    application:set_env(riak_core, web_port, 8098),
    ?assertEqual({"127.0.0.1", 8098}, http_ip_and_port()),
    %% Test the config for 0.14 and later
    application:set_env(riak_core, http, [{"localhost", 9000}]),
    ?assertEqual({"localhost", 9000}, http_ip_and_port()),

    [application:unset_env(riak_core, K) || K <- [web_ip, web_port, http]],
    ok.

default_bucket_props_test_case() ->
    DefaultBucketProps = [{allow_mult,false},
                          {chash_keyfun,{riak_core_util,chash_std_keyfun}},
                          {last_write_wins,false},
                          {n_val,3},
                          {postcommit,[]},
                          {precommit,[]}],
    application:set_env(riak_core, default_bucket_props, DefaultBucketProps),
    ?assertEqual(DefaultBucketProps, default_bucket_props()).

target_n_val_test_case() ->
    ?assertEqual(4, target_n_val()).

gossip_interval_test_case() ->
    %% Explicitly set the value because other
    %% unit tests change the default.
    application:set_env(riak_core, gossip_interval, 60000),
    ?assertEqual(60000, gossip_interval()).

cluster_name_test_case() ->
    ?assertEqual("default", cluster_name()).

ring_creation_size_test_case() ->
    %% Explicitly set the value because other
    %% unit tests change the default.
    application:set_env(riak_core, ring_creation_size, 64),
    ?assertEqual(64, ring_creation_size()).

ring_state_dir_test_case() ->
    ?assertEqual("data/ring", ring_state_dir()).

non_existent_var_test_case() ->
    ?assertEqual(undefined, get_riak_core_env(bogus)).

setup() ->   
    application:load(riak_core).

cleanup(_Pid) ->
    application:stop(riak_core).
    
-endif.
