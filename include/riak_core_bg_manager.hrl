%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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
%% NOTES:
%% The background manager allows tokens and locks to be "acquired" by
%% competing processes in a way that limits the total load on the cluster.
%%
%% The model is different than your typical semaphore. Here, we are
%% interested in coordinating background jobs that start, run, and die.
%% 
%%
%% The term "given" is a general version of "held", "acquired", or
%% "allocated" for both locks and tokens. Held doesn't make sense for
%% tokens since they aren't held. So, "given" applies to both locks
%% and tokens, but you can think "held" for locks if that's more fun.
%%
%% Resources are defined by their "names", which is the same as "type"
%% or "kind". A lock name might be the atom 'aae_hashtree_lock' or the
%% tuple '{my_ultimate_lock, 42}'.
%%
%% Usage:
%% 1. register your lock/token and set it's max concurrency/rate.
%% 2. "get" a lock/token by it's resource type/name
%% 3. do stuff
%% 4. let your process die, which gives back a lock.
%% -------------------------------------------------------------------
-type bg_lock()  :: any().
-type bg_token() :: any().
-type bg_resource()      :: bg_token() | bg_lock().
-type bg_resource_type() :: lock | token.

-type bg_meta()  :: {atom(), any()}.                %% meta data to associate with a lock/token
-type bg_period() :: pos_integer().                 %% token refill period in milliseconds
-type bg_count() :: pos_integer().                  %% token refill tokens to count at each refill period
-type bg_rate() :: {bg_period(), bg_count()}.       %% token refill rate
-type bg_concurrency_limit() :: non_neg_integer() | infinity.  %% max lock concurrency allowed
-type bg_state() :: given | blocked | failed.       %% state of an instance of a resource.

%% Results of a "ps" of live given or blocked locks/tokens
-record(bg_stat_live,
        {
          resource   :: bg_resource(),            %% resource name, e.g. 'aae_hashtree_lock'
          type       :: bg_resource_type(),       %% resource type, e.g. 'lock'
          consumer   :: pid(),                    %% process asking for token
          meta       :: [bg_meta()],              %% associated meta data
          state      :: bg_state()                %% result of last request, e.g. 'given'
        }).
-type bg_stat_live() :: #bg_stat_live{}.

%% Results of a "head" or "tail", per resource. Historical query result.
-record(bg_stat_hist,
        {
          type    :: undefined | bg_resource_type(),  %% undefined only on default
          limit   :: bg_count(),  %% maximum available, defined by token rate during interval
          refills :: bg_count(),  %% number of times a token was refilled during interval. 0 if lock
          given   :: bg_count(),  %% number of times this resource was handed out within interval
          blocked :: bg_count()   %% number of blocked processes waiting for a token
        }).
-type bg_stat_hist() :: #bg_stat_hist{}.
-define(BG_DEFAULT_STAT_HIST,
        #bg_stat_hist{type=undefined, limit=0, refills=0, given=0, blocked=0}).

-define(BG_DEFAULT_WINDOW_INTERVAL, 60).    %% in seconds
-define(BG_DEFAULT_OUTPUT_SAMPLES, 20).     %% default number of sample windows displayed
-define(BG_DEFAULT_KEPT_SAMPLES, 10000).    %% number of history samples to keep
-define(BG_ETS_TABLE, background_mgr_table).%% name of private lock/token manager ETS table
-define(BG_ETS_OPTS, [private, bag]).       %% creation time properties of token manager ETS table


