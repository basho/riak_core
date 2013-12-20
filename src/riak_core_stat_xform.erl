%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_core_stat_xform).

-export([parse_transform/2]).


parse_transform(Forms, _) ->
    L = [{prefix     , env("RIAK_CORE_STAT_PREFIX")},
         {stat_system, env("RIAK_CORE_STAT_SYSTEM")}],
    transform(Forms, [X || {_,V} = X <- L, V =/= undefined]).

env(Var) ->
    case os:getenv(Var) of
	false -> undefined;
	Str ->
	    list_to_atom(Str)
    end.

transform([{function,L,F,0,_}|T] = Form, L) ->
    case lists:keyfind(F, 1, L) of
        {_, V} when is_atom(V) ->
            [{function, L, F, 0,
              [{clause, L, [], [], [{atom,L,V}]}]}|T];
        false ->
            Form
    end;
transform([H|T], Pfx) ->
    [H | transform(T, Pfx)];
transform([], _) ->
    [].


