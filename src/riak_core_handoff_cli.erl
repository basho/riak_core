%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_core_handoff_cli).

-behavior(riak_cli_handler).

-export([register_cli/0]).

-spec register_cli() -> ok.
register_cli() ->
    register_cli_usage(),
    register_cli_cfg(),
    register_cli_cmds().

register_cli_cmds() ->
    CmdList = [handoff_cmd_spec(EnOrDis, Dir) ||
               EnOrDis <- [enable, disable],
               Dir     <- [inbound, outbound, both]],
    lists:foreach(fun(Args) -> apply(riak_cli, register_command, Args) end,
                  CmdList).

register_cli_cfg() ->
    lists:foreach(fun(K) ->
                          riak_cli:register_config(K, fun handoff_cfg_change_callback/3)
                  end, [["handoff", "disable_inbound"], ["handoff", "disable_outbound"]]).

register_cli_usage() ->
    riak_cli:register_usage(["riak-admin", "handoff"], handoff_usage()),
    riak_cli:register_usage(["riak-admin", "handoff", "enable"], handoff_enable_disable_usage()),
    riak_cli:register_usage(["riak-admin", "handoff", "disable"], handoff_enable_disable_usage()).

handoff_usage() ->
    ["riak-admin handoff <subcommand> [args]\n\n",
     "Currently implemented handoff commands are:\n",
     "  enable   Enable handoffs for the specified node(s)\n",
     "  disable  Disable handoffs for the specified node(s)\n"
    ].

handoff_enable_disable_usage() ->
    ["riak-admin handoff <enable | disable> <inbound | outbound | both> ",
     "[[--node | -n] <Node>] [--all]\n\n",
     "  Enable or disable handoffs on the specified node(s).\n",
     "  If handoffs are disabled in a direction, any currently\n",
     "  running handoffs in that direction will be terminated.\n\n"
     "Options\n",
     "  -n <Node>, --node <Node>\n",
     "      Modify the setting on the specified node (default: local node only)\n",
     "  -a, --all\n",
     "      Modify the setting on every node in the cluster\n"
    ].

handoff_cmd_spec(EnOrDis, Direction) ->
    Cmd = ["riak-admin", "handoff", atom_to_list(EnOrDis), atom_to_list(Direction)],
    Callback = fun([], Flags) ->
                       handoff_change_enabled_setting(EnOrDis, Direction, Flags)
               end,
    [
     Cmd,
     [], % KeySpecs
     [{all, [{shortname, "a"},
             {longname, "all"}]},
      {node, [{shortname, "n"},
              {longname, "node"}]}], % FlagSpecs
     Callback
    ].

handoff_change_enabled_setting(_EnOrDis, _Direction, Flags) when length(Flags) > 1 ->
    [riak_cli_status:text("Can't specify both --all and --node flags")];
handoff_change_enabled_setting(EnOrDis, Direction, [{all, _}]) ->
    Nodes = riak_cli_nodes:nodes(),
    {_, Down} = rpc:multicall(Nodes,
                              riak_core_handoff_manager,
                              handoff_change_enabled_setting,
                              [EnOrDis, Direction],
                              60000),

    case Down of
        [] ->
            [riak_cli_status:text("All nodes successfully updated")];
        _ ->
            Output = io_lib:format("Handoff ~s failed on nodes: ~p", [EnOrDis, Down]),
            [riak_cli_status:alert([riak_cli_status:text(Output)])]
    end;
handoff_change_enabled_setting(EnOrDis, Direction, [{node, NodeStr}]) ->
    Node = riak_cli_typecast:to_node(NodeStr),
    Result = riak_cli_nodes:safe_rpc(Node,
                                     riak_core_handoff_manager, handoff_change_enabled_setting,
                                     [EnOrDis, Direction]),
    case Result of
        {badrpc, Reason} ->
            Output = io_lib:format("Failed to update handoff settings on node ~p. Reason: ~p",
                                   [Node, Reason]),
            [riak_cli_status:alert([riak_cli_status:text(Output)])];
        _ ->
            [riak_cli_status:text("Handoff setting successfully updated")]
    end;

handoff_change_enabled_setting(EnOrDis, Direction, []) ->
    riak_core_handoff_manager:handoff_change_enabled_setting(EnOrDis, Direction),
    [riak_cli_status:text("Handoff setting successfully updated")].

handoff_cfg_change_callback(["handoff", Cmd], "off", _Flags) ->
    case Cmd of
        "disable_inbound" ->
            riak_core_handoff_manager:kill_handoffs_in_direction(inbound);
        "disable_outbound" ->
            riak_core_handoff_manager:kill_handoffs_in_direction(outbound)
    end;
handoff_cfg_change_callback(_, _, _) ->
    ok.

