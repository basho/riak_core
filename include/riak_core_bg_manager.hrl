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
-type bg_lock()  :: any().
-type bg_token() :: any().
-type bg_resource()      :: bg_token() | bg_lock().
-type bg_resource_type() :: lock | token.

-type bg_meta()  :: undefined | {atom(), any()}.    %% meta data to associate with a lock/token
-type bg_period() :: pos_integer().                 %% token refill period in milliseconds
-type bg_count() :: pos_integer().                  %% token refill tokens to count at each refill period
-type bg_rate() :: undefined | {bg_period(), bg_count()}.       %% token refill rate
-type bg_concurrency_limit() :: non_neg_integer() | infinity.   %% max lock concurrency allowed
-type bg_consumer() :: {undefined | pid(), bg_meta()}.          %% a consumer of a resource

%% Results of a "ps" of live given or blocked locks/tokens
-record(bg_stat_live,
        {
          resource   :: bg_resource(),            %% resource name, e.g. 'aae_hashtree_lock'
          type       :: bg_resource_type(),       %% resource type, e.g. 'lock'
          owner      :: bg_consumer()             %% this consumer has the lock or token
        }).
-type bg_stat_live() :: #bg_stat_live{}.

-define(BG_INFO_ETS_TABLE, background_mgr_info_table).  %% name of lock/token manager info ETS table
-define(BG_INFO_ETS_OPTS, [public,                   %% creation time properties of info ETS table
                           named_table,
                           set]).

-define(BG_ENTRY_ETS_TABLE, background_mgr_entry_table). %% name of lock/token manager entry ETS table
-define(BG_ENTRY_ETS_OPTS, [public,                   %% creation time properties of entry ETS table
                            named_table,
                            bag]).


