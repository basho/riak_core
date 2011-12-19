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
%%
%% @doc This module is a pass-thru to `riak_core_claim' for backwards
%% compatability.

-module(riak_core_new_claim).
-export([new_wants_claim/2, new_choose_claim/2]).

%% @deprecated
%%
%% @doc This exists for the sole purpose of backwards compatability.
new_wants_claim(Ring, Node) ->
    riak_core_claim:wants_claim_v2(Ring, Node).

%% @deprecated
%%
%% @doc This exists for the sole purpose of backwards compatability.
new_choose_claim(Ring, Node) ->
    riak_core_claim:choose_claim_v2(Ring, Node).
