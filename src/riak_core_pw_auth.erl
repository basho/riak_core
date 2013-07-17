%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
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

-module(riak_core_pw_auth).

%% TODO
-compile(export_all).

%% TOOD should make these configurable
-define(SALT_LENGTH, 16).
-define(DERIVED_LENGTH, 32).
-define(HASH_ITERATIONS, 65536).
-define(HASH_FUNCTION, {hmac, sha}).

hash_password(Password) ->
    %% TODO: Do something more with the salt?
    %% Generate salt the simple way
    Salt = crypto:rand_bytes(?SALT_LENGTH),
    
    BinaryPass = list_to_binary(Password),
    
    %% Hash the original password
    lager:info("Calling pbkdf2 with the following:"),
    lager:info("hash_func = ~p", [?HASH_FUNCTION]),
    lager:info("password = ~p", [BinaryPass]),
    lager:info("salt = ~p", [Salt]),
    lager:info("iterations = ~p", [?HASH_ITERATIONS]),
    lager:info("length = ~p", [?DERIVED_LENGTH]),
    {ok, HashedPass} = pbkdf2:pbkdf2(?HASH_FUNCTION, BinaryPass, Salt, ?HASH_ITERATIONS, ?DERIVED_LENGTH),
    {ok, HashedPass, <<"pbkdf2">>, ?HASH_FUNCTION, Salt, ?HASH_ITERATIONS, ?DERIVED_LENGTH}.
