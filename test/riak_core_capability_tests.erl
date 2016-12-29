%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
%%
%% Copyright (c) 2007-2017 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_core_capability_tests).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

basic_test() ->
    S1 = riak_core_capability:init_state([]),

    S2 = riak_core_capability:register_capability(n1,
                             {riak_core, test},
                             riak_core_capability:capability_info([x,a,c,y], y, []),
                             S1),
    S3 = riak_core_capability:add_node_capabilities(n2,
                               [{{riak_core, test}, [a,b,c,y]}],
                               S2),
    S4 = riak_core_capability:negotiate_capabilities(n1, [{riak_core, []}], S3),
    ?assertEqual([{{riak_core, test}, a}], riak_core_capability:get_negotiated(S4)),

    S5 = riak_core_capability:negotiate_capabilities(n1,
                                [{riak_core, [{test, [{prefer, c}]}]}],
                                S4),
    ?assertEqual([{{riak_core, test}, c}], riak_core_capability:get_negotiated(S5)),

    S6 = riak_core_capability:add_node_capabilities(n3,
                               [{{riak_core, test}, [b]}],
                               S5),
    S7 = riak_core_capability:negotiate_capabilities(n1, [{riak_core, []}], S6),
    ?assertEqual([{{riak_core, test}, y}], riak_core_capability:get_negotiated(S7)),

    S8 = riak_core_capability:negotiate_capabilities(n1,
                                [{riak_core, [{test, [{use, x}]}]}],
                                S7),
    ?assertEqual([{{riak_core, test}, x}], riak_core_capability:get_negotiated(S8)),
    ok.

-endif.
