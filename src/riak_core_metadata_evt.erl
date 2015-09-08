%% -------------------------------------------------------------------
%% 
%% An API for a gen_event event manager for bucket type events.
%%
%% Copyright (c) 2015 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_core_metadata_evt).

%%% ------------------------------------------------------------------
%%% API Function Exports
%%% ------------------------------------------------------------------

-export([start_link/0]).
-export([swap_handler/2]).
-export([sync_notify_stored/1]).

%%% ------------------------------------------------------------------
%%% API Function Definitions
%%% ------------------------------------------------------------------

%%
start_link() ->
    gen_event:start_link({local, ?MODULE}).

%% Swap the existing `Handler' with the given one, or create a new one
%% if one does not exist. Prevents event handlers being added multiple
%% times. 
swap_handler(Handler, Args) ->
    Terminate_args = [],
    gen_event:swap_handler(
        ?MODULE, {Handler, Terminate_args}, {Handler, Args}).

%% Notify all listeners that metadata has been stored, this could mean
%% that a new bucket type has been created or that it has been updated.
%% Listeners receive the event type `{metadata_stored, Bucket_type :: binary()}'.
sync_notify_stored(Bucket_type) when is_binary(Bucket_type) ->
    gen_event:sync_notify(?MODULE, {metadata_stored, Bucket_type}).
