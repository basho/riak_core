%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_core_app).

-behaviour(application).

%% Application callbacks
-export([start/2, prep_stop/1, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    %% Don't add our system_monitor event handler here.  Instead, let
    %% riak_core_sysmon_minder start it, because that process can act
    %% on any handler crash notification, whereas we cannot.

    case application:get_env(riak_core, delayed_start) of
        {ok, Delay} ->
            lager:info("Delaying riak_core startup as requested"),
            timer:sleep(Delay);
        _ ->
            ok
    end,

    %% Validate that the ring state directory exists
    riak_core_util:start_app_deps(riak_core),
    RingStateDir = app_helper:get_env(riak_core, ring_state_dir),
    case filelib:ensure_dir(filename:join(RingStateDir, "dummy")) of
        ok ->
            ok;
        {error, RingReason} ->
            lager:critical(
              "Ring state directory ~p does not exist, "
              "and could not be created: ~p",
              [RingStateDir, lager:posix_error(RingReason)]),
            throw({error, invalid_ring_state_dir})
    end,

    %% Register our cluster_info app callback modules, with catch if
    %% the app is missing or packaging is broken.
    catch cluster_info:register_app(riak_core_cinfo_core),

    %% add these defaults now to supplement the set that may have been
    %% configured in app.config
    riak_core_bucket:append_bucket_defaults(
      [{n_val,3},
       {allow_mult,false},
       {last_write_wins,false},
       {precommit, []},
       {postcommit, []},
       {chash_keyfun, {riak_core_util, chash_std_keyfun}}]),

    %% Spin up the supervisor; prune ring files as necessary
    case riak_core_sup:start_link() of
        {ok, Pid} ->
            riak_core:register(riak_core, [{stat_mod, riak_core_stat}]),
            ok = riak_core_ring_events:add_guarded_handler(riak_core_ring_handler, []),

            %% Register capabilities
            riak_core_capability:register({riak_core, vnode_routing},
                                          [proxy, legacy],
                                          legacy,
                                          {riak_core,
                                           legacy_vnode_routing,
                                           [{true, legacy}, {false, proxy}]}),
            riak_core_capability:register({riak_core, staged_joins},
                                          [true, false],
                                          false),
            riak_core_capability:register({riak_core, resizable_ring},
                                          [true, false],
                                          false),

            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Prepare to stop - called before the supervisor tree is shutdown
prep_stop(_State) ->
    try %% wrap with a try/catch - application carries on regardless,
        %% no error message or logging about the failure otherwise.
        lager:info("Stopping application riak_core - disabling web services.\n", []),
        riak_core_sup:stop_webs()
    catch
        Type:Reason ->
            lager:error("Stopping application riak_core - ~p:~p.\n", [Type, Reason])
    end,
    stopping.

stop(_State) ->
    lager:info("Stopped  application riak_core.\n", []),
    ok.
