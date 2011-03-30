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
-module(riak_core).
-export([stop/0, stop/1]).
-export([register_vnode_module/1, vnode_modules/0]).

%% @spec stop() -> ok
%% @doc Stop the riak application and the calling process.
stop() -> stop("riak stop requested").

-ifdef(TEST).
stop(Reason) ->
    error_logger:info_msg(io_lib:format("~p~n",[Reason])),
    % if we're in test mode, we don't want to halt the node, so instead
    % we just stop the application.
    application:stop(riak_core).
-else.
stop(Reason) ->
    % we never do an application:stop because that makes it very hard
    %  to really halt the runtime, which is what we need here.
    error_logger:info_msg(io_lib:format("~p~n",[Reason])),
    init:stop().
-endif.

vnode_modules() ->
    case application:get_env(riak_core, vnode_modules) of
        undefined -> [];
        {ok, Mods} -> Mods
    end.

register_vnode_module(VNodeMod) when is_atom(VNodeMod)  ->
    {ok, App} = case application:get_application(self()) of
        {ok, AppName} -> {ok, AppName};
        undefined -> app_for_module(VNodeMod)                        
    end,
    case application:get_env(riak_core, vnode_modules) of
        undefined ->
            application:set_env(riak_core, vnode_modules, [{App,VNodeMod}]);
        {ok, Mods} ->
            application:set_env(riak_core, vnode_modules, [{App,VNodeMod}|Mods])
    end,
    riak_core_ring_events:force_sync_update().
    

app_for_module(Mod) ->
    app_for_module(application:which_applications(), Mod).

app_for_module([], _Mod) ->
    undefined;
app_for_module([{App,_,_}|T], Mod) ->
    {ok, Mods} = application:get_key(App, modules),
    case lists:member(Mod, Mods) of
        true -> {ok, App};
        false -> app_for_module(T, Mod)
    end.
