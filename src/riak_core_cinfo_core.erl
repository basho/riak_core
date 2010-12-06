%% -------------------------------------------------------------------
%%
%% Riak: A lightweight, decentralized key-value store.
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
-module(riak_core_cinfo_core).

-export([cluster_info_init/0, cluster_info_generator_funs/0]).

%% @spec () -> term()
%% @doc Required callback function for cluster_info: initialization.
%%
%% This function doesn't have to do anything.

cluster_info_init() ->
    ok.

%% @spec () -> list({string(), fun()})
%% @doc Required callback function for cluster_info: return list of
%%      {NameForReport, FunOfArity_1} tuples to generate ASCII/UTF-8
%%      formatted reports.

cluster_info_generator_funs() ->
    [
     {"Riak Core config files", fun config_files/1},
     {"Riak Core vnode modules", fun vnode_modules/1},
     {"Riak Core ring", fun get_my_ring/1},
     {"Riak Core latest ring file", fun latest_ringfile/1},
     {"Riak Core active partitions", fun active_partitions/1}
    ].

vnode_modules(CPid) -> % CPid is the data collector's pid.
    cluster_info:format(CPid, "~p\n", [riak_core:vnode_modules()]).

get_my_ring(CPid) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    cluster_info:format(CPid, "~p\n", [Ring]).

latest_ringfile(CPid) ->
    {ok, Path} = riak_core_ring_manager:find_latest_ringfile(),
    {ok, Contents} = file:read_file(Path),
    cluster_info:format(CPid, "Latest ringfile: ~s\n", [Path]),
    cluster_info:format(CPid, "File contents:\n~p\n", [binary_to_term(Contents)]).

active_partitions(CPid) ->
    Pids = [Pid || {_,Pid,_,_} <- supervisor:which_children(riak_core_vnode_sup)],
    Vnodes = [riak_core_vnode:get_mod_index(Pid) || Pid <- Pids],
    Partitions = lists:foldl(fun({_,P}, Ps) -> 
                                     ordsets:add_element(P, Ps)
                             end, ordsets:new(), Vnodes),
    cluster_info:format(CPid, "~p\n", [Partitions]).

config_files(C) ->
    {ok, [[AppPath]]} = init:get_argument(config),
    EtcDir = filename:dirname(AppPath),
    VmPath = filename:join(EtcDir, "vm.args"),
    [begin
         cluster_info:format(C, "File: ~s\n", [os:cmd("ls -l " ++ File)]),
         {ok, FileBin} = file:read_file(File),
         cluster_info:format(C, "File contents:\n~s\n", [FileBin])
     end || File <- [AppPath, VmPath]].

