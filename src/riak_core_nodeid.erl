%% -------------------------------------------------------------------
%%
%% riak_core: Core Node Id
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
%% Return a binary to use as an identifier for this node.
%% Initially this is just hash the node name, so there is a small
%% chance of collisions.  
%% -------------------------------------------------------------------
-module(riak_core_nodeid).
-export([get/0]).

get() ->
    Id = erlang:crc32(term_to_binary(node())),
    <<Id:32/unsigned-integer>>.
