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

-behavior(clique_handler).

-export([register_cli/0]).

-spec register_cli() -> ok.
register_cli() ->
    register_cli_usage(),
    register_cli_cfg(),
    register_cli_cmds(),
    register_config_whitelist(),
    ok.

register_cli_cmds() ->
    register_enable_disable_commands(),
    ok = clique:register_command(["riak-admin", "handoff", "summary"], [], [],
                                 fun riak_core_handoff_status:handoff_summary/3),
    ok = clique:register_command(["riak-admin", "handoff", "details"], [],
                                 node_and_all_flags(), fun riak_core_handoff_status:handoff_details/3),
    ok = clique:register_command(["riak-admin", "handoff", "config"], [],
                                 node_and_all_flags(), fun handoff_config/3).

node_and_all_flags() ->
    [{node, [{shortname, "n"}, {longname, "node"},
             {typecast, fun clique_typecast:to_node/1}]},
     {all, [{shortname, "a"}, {longname, "all"}]}].

register_enable_disable_commands() ->
    CmdList = [handoff_cmd_spec(EnOrDis, Dir) ||
                  EnOrDis <- [enable, disable],
                  Dir <- [inbound, outbound, both]],
    lists:foreach(fun(Args) -> apply(clique, register_command, Args) end,
                  CmdList).

register_cli_cfg() ->
    lists:foreach(fun(K) ->
                          clique:register_config(K, fun handoff_cfg_change_callback/2)
                  end, [["handoff", "inbound"], ["handoff", "outbound"]]),
    clique:register_config(["transfer_limit"], fun set_transfer_limit/2).

register_config_whitelist() ->
    ok = clique:register_config_whitelist(["transfer_limit",
                                           "handoff.outbound",
                                           "handoff.inbound"]).

register_cli_usage() ->
    clique:register_usage(["riak-admin", "handoff"], handoff_usage()),
    clique:register_usage(["riak-admin", "handoff", "enable"], handoff_enable_disable_usage()),
    clique:register_usage(["riak-admin", "handoff", "disable"], handoff_enable_disable_usage()),
    clique:register_usage(["riak-admin", "handoff", "summary"], summary_usage()),
    clique:register_usage(["riak-admin", "handoff", "details"], details_usage()),
    clique:register_usage(["riak-admin", "handoff", "config"], config_usage()).

handoff_usage() ->
    [
      "riak-admin handoff <sub-command>\n\n",
      "  Display handoff-related status and settings.\n\n",
      "  Sub-commands:\n",
      "    enable     Enable handoffs for the specified node(s)\n",
      "    disable    Disable handoffs for the specified node(s)\n"
      "    summary    Show cluster-wide handoff summary\n",
      "    details    Show details of all active transfers (per-node or cluster wide)\n",
      "    config     Show all configuration for handoff subsystem\n\n",
      "  Use --help after a sub-command for more details.\n"
    ].

config_usage() ->
    ["riak-admin handoff config\n\n",
     "  Display handoff related configuration variables\n\n",
     "Options\n",
     "  -n <node>, --node <node>\n",
     "      Show the settings on the specified node.\n",
     "      This flag can currently take only one node and be used once\n"
     "  -a, --all\n",
     "      Show the settings on every node in the cluster\n"
    ].

handoff_enable_disable_usage() ->
    ["riak-admin handoff <enable|disable> <inbound|outbound|both> ",
     "[-n <node>|--all]\n\n",
     "  Enable or disable handoffs on the local or specified node(s).\n",
     "  If handoffs are disabled in a direction, any currently\n",
     "  running handoffs in that direction will be terminated.\n\n"
     "Options\n",
     "  -n <node>, --node <node>\n",
     "      Modify the setting on the specified node.\n",
     "      This flag can currently take only one node and be used once\n"
     "  -a, --all\n",
     "      Modify the setting on every node in the cluster\n"
    ].

handoff_cmd_spec(EnOrDis, Direction) ->
    Cmd = ["riak-admin", "handoff", atom_to_list(EnOrDis), atom_to_list(Direction)],
    Callback = fun(_, [], Flags) ->
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

summary_usage() ->
    [
     "riak-admin handoff summary\n\n",
     "  Display a summarized view of handoffs.\n"
    ].

details_usage() ->
    [
     "riak-admin handoff details [--node <node>|--all]\n\n",
     "  Display a detailed list of handoffs. Defaults to local node.\n\n"
     "Options\n",
     "  -n <node>, --node <node>\n",
     "      Display the handoffs on the specified node.\n",
     "      This flag can currently take only one node and be used once\n"
     "  -a, --all\n",
     "      Display the handoffs on every node in the cluster\n"
    ].

handoff_config(_CmdBase, _Args, Flags) when length(Flags) > 1 ->
    [clique_status:text("Can't specify both --all and --node flags")];
handoff_config(_CmdBase, _Args, []) ->
    clique_config:show(config_vars(), []);
handoff_config(_CmdBase, _Args, [{all, Val}]) ->
    clique_config:show(config_vars(), [{all, Val}]);
handoff_config(_CmdBase, _Args, [{node, Node}]) ->
    clique_config:show(config_vars(), [{node, Node}]).

config_vars() ->
    ["transfer_limit", "handoff.outbound", "handoff.inbound", "handoff.port"].

handoff_change_enabled_setting(_EnOrDis, _Direction, Flags) when length(Flags) > 1 ->
    [clique_status:text("Can't specify both --all and --node flags")];
handoff_change_enabled_setting(EnOrDis, Direction, [{all, _}]) ->
    Nodes = clique_nodes:nodes(),
    {_, Down} = rpc:multicall(Nodes,
                              riak_core_handoff_manager,
                              handoff_change_enabled_setting,
                              [EnOrDis, Direction],
                              60000),

    case Down of
        [] ->
            [clique_status:text("All nodes successfully updated")];
        _ ->
            Output = io_lib:format("Handoff ~s failed on nodes: ~p", [EnOrDis, Down]),
            [clique_status:alert([clique_status:text(Output)])]
    end;
handoff_change_enabled_setting(EnOrDis, Direction, [{node, NodeStr}]) ->
    Node = clique_typecast:to_node(NodeStr),
    Result = clique_nodes:safe_rpc(Node,
                                     riak_core_handoff_manager, handoff_change_enabled_setting,
                                     [EnOrDis, Direction]),
    case Result of
        {badrpc, Reason} ->
            Output = io_lib:format("Failed to update handoff settings on node ~p. Reason: ~p",
                                   [Node, Reason]),
            [clique_status:alert([clique_status:text(Output)])];
        _ ->
            [clique_status:text("Handoff setting successfully updated")]
    end;

handoff_change_enabled_setting(EnOrDis, Direction, []) ->
    riak_core_handoff_manager:handoff_change_enabled_setting(EnOrDis, Direction),
    [clique_status:text("Handoff setting successfully updated")].

handoff_cfg_change_callback(["handoff", Cmd], "off") ->
    case Cmd of
        "inbound" ->
            riak_core_handoff_manager:kill_handoffs_in_direction(inbound),
            "Inbound handoffs terminated";
        "outbound" ->
            riak_core_handoff_manager:kill_handoffs_in_direction(outbound),
            "Outbound handoffs terminated"
    end;
handoff_cfg_change_callback(_, _) ->
    "".

set_transfer_limit(["transfer_limit"], LimitStr) ->
    Limit = list_to_integer(LimitStr),
    riak_core_handoff_manager:set_concurrency(Limit),
    "".
