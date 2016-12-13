%% -------------------------------------------------------------------
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
%% -------------------------------------------------------------------
-module(riak_core_base64url_tests).

-include_lib("eunit/include/eunit.hrl").

-define(URL, "http://example.com/foo?query=thing").

string_to_string_test() ->
    Encoded = riak_core_base64url:encode_to_string(?URL),
    Decoded = riak_core_base64url:decode_to_string(Encoded),
    ?assertEqual(?URL, Decoded).

string_to_binary_test() ->
    Encoded = riak_core_base64url:encode(?URL),
    Decoded = riak_core_base64url:decode(Encoded),
    ?assertEqual(<<?URL>>, Decoded).

binary_to_binary_test() ->
    Encoded = riak_core_base64url:encode(<<?URL>>),
    Decoded = riak_core_base64url:decode(Encoded),
    ?assertEqual(<<?URL>>, Decoded).

binary_to_string_test() ->
    Encoded = riak_core_base64url:encode_to_string(<<?URL>>),
    Decoded = riak_core_base64url:decode_to_string(Encoded),
    ?assertEqual(?URL, Decoded).
