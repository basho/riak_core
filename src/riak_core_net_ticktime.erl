%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
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

%% @doc Change net_kernel's ticktime on-the-fly.

-module(riak_core_net_ticktime).
-export([start_set_net_ticktime_daemon/2,
         stop_set_net_ticktime_daemon/1]).

-define(REGNAME, net_kernel_net_ticktime_change_daemon).

start_set_net_ticktime_daemon(Node, Time) ->
    EbinDir = filename:dirname(code:which(?MODULE)),
    try
        Dirs = rpc:call(Node, code, get_path, []),
        case lists:member(EbinDir, Dirs) of
            false ->
                lager:info("start_set_net_ticktime_daemon: adding to code path "
                           "for node ~p\n", [Node]),
                rpc:call(Node, code, add_pathz, [EbinDir]);
            true ->
                ok
        end
    catch _:_ ->
            %% Network problems or timeouts here, spawn will fail, no
            %% worries, we'll try again soon.
            ok
    end,
    spawn(Node, fun() ->
                        try
                            register(?REGNAME, self()),
                            %% If we get here, we are the one daemon process
                            lager:info("start_set_net_ticktime_daemon: started "
                                       "changing net_ticktime on ~p to ~p\n",
                                 [Node, Time]),
                            _ = random:seed(os:timestamp()),
                            set_net_ticktime_daemon_loop(Time, 1)
                        catch _:_ ->
                                ok
                        end
                end).

stop_set_net_ticktime_daemon(Node) ->
    try
        case rpc:call(Node, erlang, whereis, [?REGNAME]) of
            Pid when is_pid(Pid) ->
                io:format("Stopping tick daemon ~p on ~p\n", [Pid, Node]),
                exit(Pid, stop_now),
                ok;
            undefined ->
                io:format("Stopping tick daemon on ~p but not running\n", [Node]),
                ok
        end
    catch _:_ ->
            %% Network problems or timeouts, we don't try too hard
            error
    end.

async_start_set_net_ticktime_daemons(Time, Nodes) ->
    Pids = [spawn(fun() ->
                          start_set_net_ticktime_daemon(Node, Time)
                  end) || Node <- Nodes],
    spawn(fun() ->
                  %% If a daemon cannot finish in 5 seconds, no worries.
                  %% We want to avoid leaving lots of pids around due to
                  %% network/net_kernel instability.
                  timer:sleep(5000),
                  [exit(Pid, kill) || Pid <- Pids]
          end).

set_net_ticktime_daemon_loop(Time, Count) ->
    case set_net_ticktime(Time) of
        unchanged ->
            lager:info("start_set_net_ticktime_daemon: finished "
                       "changing net_ticktime on ~p to ~p\n", [node(), Time]),
            exit(normal);
        _ ->
            timer:sleep(random:uniform(1*1000)),
            %% Here is an uncommon use the erlang:nodes/1 BIF.
            %% Hidden nodes (e.g. administrative escripts) may have
            %% connected to us.  Force them to change their tick time,
            %% in case they're using something different.  And pick up
            %% any regular nodes that have connected since we started.
            if
                Count rem 5 == 0 ->
                    async_start_set_net_ticktime_daemons(
                      Time, da_nodes(nodes(connected))),
                    ok;
                true ->
                    ok
            end,
            set_net_ticktime_daemon_loop(Time, Count + 1)
    end.

da_nodes(Nodes) ->
    lists:sort([node()|Nodes]).                 % Always include myself

set_net_ticktime(Time) ->
    case net_kernel:set_net_ticktime(Time) of
        {Status, _} ->
            Status;
        A when is_atom(A) ->
            A
    end.

