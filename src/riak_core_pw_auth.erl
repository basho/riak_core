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

-export([hash_password/1, check_password/5]).

%% TOOD should make these configurable in app.config
-define(SALT_LENGTH, 16).
-define(HASH_ITERATIONS, 65536).
%% TODO this should call a default_hash_func() function to get default based on erlang version
-define(HASH_FUNCTION, sha).
-define(AUTH_NAME, pbkdf2).

%% @doc Hash a plaintext password, returning hashed password and algorithm details
hash_password(BinaryPass) when is_binary(BinaryPass) ->
    % TODO: Do something more with the salt?
    % Generate salt the simple way
    Salt = riak_core_rand:rand_bytes(?SALT_LENGTH),

    % Hash the original password and store as hex
    {ok, HashedPass} = pbkdf2:pbkdf2(?HASH_FUNCTION, BinaryPass, Salt, ?HASH_ITERATIONS),
    HexPass = pbkdf2:to_hex(HashedPass),
    {ok, HexPass, ?AUTH_NAME, ?HASH_FUNCTION, Salt, ?HASH_ITERATIONS}.


%% @doc Check a plaintext password with a hashed password
check_password(BinaryPass, HashedPassword, HashFunction, Salt, HashIterations) when is_binary(BinaryPass) ->

    % Hash EnteredPassword to compare to HashedPassword
    {ok, HashedPass} = pbkdf2:pbkdf2(HashFunction, BinaryPass, Salt, HashIterations),
    HexPass = pbkdf2:to_hex(HashedPass),
    pbkdf2:compare_secure(HexPass, HashedPassword).
