%%%----------------------------------------------------------------------
%%% Copyright: (c) 2009-2010 Gemini Mobile Technologies, Inc.  All rights reserved.
%%% Copyright: (c) 2010 Basho Technologies, Inc.  All rights reserved.
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%----------------------------------------------------------------------

-module(riak_core_cinfo_basic).

%% Registration API
-export([register/0]).

%% Mandatory callbacks.
-export([cluster_info_init/0, cluster_info_generator_funs/0]).

%% Export in case anyone else thinks they might be useful.
-export([alarm_info/1, application_info/1,
         capture_ets_i/1, capture_net_kernel_i/1, nodes_info/1, capture_regs/1,
         central_conf/1,
         erlang_memory/1, erlang_statistics/1,
         erlang_system_info/1, global_summary/1, inet_db_summary/1,
         loaded_modules/1, memory_hogs/2, non_zero_mailboxes/1, port_info/1,
         registered_names/1, time_and_date/1, timer_status/1]).

register() ->
    cluster_info:register_app(?MODULE).

cluster_info_init() ->
    ok.

cluster_info_generator_funs() ->
    [
     %% Short(er) output items at the top
     {"Current time and date", fun time_and_date/1},
     {"VM statistics", fun erlang_statistics/1},
     {"erlang:memory() summary", fun erlang_memory/1},
     {"Top 50 process memory hogs", fun(C) -> memory_hogs(C, 50) end},
     {"Registered process names", fun registered_names/1},
     {"Registered process name via regs()", fun capture_regs/1},
     {"Non-zero mailbox sizes", fun non_zero_mailboxes/1},
     {"Ports", fun port_info/1},
     {"Applications", fun application_info/1},
     {"Timer status", fun timer_status/1},
     {"ETS summary", fun capture_ets_i/1},
     {"Nodes summary", fun nodes_info/1},
     {"net_kernel summary", fun capture_net_kernel_i/1},
     {"inet_db summary", fun inet_db_summary/1},
     {"Global summary", fun global_summary/1},
     {"Application config (central.conf)", fun central_conf/1},

     %% Longer output starts here.
     {"erlang:system_info() summary", fun erlang_system_info/1},
     {"Loaded modules", fun loaded_modules/1}
    ].

alarm_info(C) ->
    Alarms = gmt_util:get_alarms(),
    cluster_info:format(C, " Number of alarms: ~p\n", [length(Alarms)]),
    cluster_info:format(C, " ~p\n", [Alarms]).

application_info(C) ->
    cluster_info:format(C, " Application summary:\n"),
    cluster_info:format(C, " ~p\n", [application:info()]),
    [try
         cluster_info:format(C, " Application:get_all_key(~p):\n", [App]),
         cluster_info:format(C, " ~p\n\n", [application:get_all_key(App)]),
         cluster_info:format(C, " Application:get_all_env(~p):\n", [App]),
         cluster_info:format(C, " ~p\n\n", [application:get_all_env(App)])
     catch X:Y ->
             cluster_info:format(C, "Error for ~p: ~p ~p at ~p\n",
                                 [App, X, Y, erlang:get_stacktrace()])
     end || App <- lists:sort([A || {A, _, _} <- application:which_applications()])].

capture_ets_i(C) ->
    %% Use the captured IO as the format string: it has \n embedded.
    cluster_info:format(
      C, cluster_info:capture_io(1000, fun() -> ets:i() end)).

capture_net_kernel_i(C) ->
    %% Use the captured IO as the format string: it has \n embedded.
    cluster_info:format(
      C, cluster_info:capture_io(1000, fun() -> net_kernel:i() end)).

capture_regs(C) ->
    %% Use the captured IO as the format string: it has \n embedded.
    cluster_info:format(
      C, cluster_info:capture_io(1000, fun() -> shell_default:regs() end)).

central_conf(C) ->
    {ok, Config} = file:read_file(gmt_config_svr:get_config_path()),
    cluster_info:send(C, ["\n", Config, "\n"]).

erlang_memory(C) ->
    cluster_info:format(C, " ~p\n", [erlang:memory()]).

erlang_statistics(C) ->
    [cluster_info:format(C, " ~p: ~p\n", [Type, catch erlang:statistics(Type)])
     || Type <- [context_switches, exact_reductions, garbage_collection,
                 io, reductions, run_queue, runtime, wall_clock]].

erlang_system_info(C) ->
    Allocs = erlang:system_info(alloc_util_allocators),
    I0 = lists:flatten(
             [allocated_areas, allocator,
              [{allocator, Alloc} || Alloc <- Allocs],
              [{allocator_sizes, Alloc} || Alloc <- Allocs],
              c_compiler_used, check_io, compat_rel, cpu_topology,
              [{cpu_topology, X} || X <- [defined, detected]],
              creation, debug_compiled, dist_ctrl,
              driver_version, elib_malloc, fullsweep_after,
              garbage_collection, global_heaps_size, heap_sizes,
              heap_type, kernel_poll,
              logical_processors, machine, modified_timing_level,
              multi_scheduling, multi_scheduling_blockers,
              otp_release, process_count, process_limit,
              scheduler_bind_type, scheduler_bindings,
              scheduler_id, schedulers, schedulers_online,
              smp_support, system_version, system_architecture,
              threads, thread_pool_size, trace_control_word,
              version, wordsize]),
    [cluster_info:format(C, " ~p:\n ~p\n\n", [I,catch erlang:system_info(I)]) ||
        I <- I0],

    I1 = [dist, info, loaded, procs],
    [cluster_info:format(C, " ~p:\n ~s\n\n", [I, catch erlang:system_info(I)]) ||
        I <- I1].

global_summary(C) ->
    cluster_info:format(C, " info: ~p\n", [global:info()]),
    cluster_info:format(C, " registered_names:\n"),
    [try
         Pid = global:whereis_name(Name),
         cluster_info:format(C, "    Name ~p, pid ~p, node ~p\n",
                             [Name, Pid, node(Pid)])
     catch _:_ ->
             ok
     end || Name <- global:registered_names()],
    ok.

inet_db_summary(C) ->
    cluster_info:format(C, " gethostname: ~p\n", [inet_db:gethostname()]),
    cluster_info:format(C, " get_rc: ~p\n", [inet_db:get_rc()]).

loaded_modules(C) ->
    cluster_info:format(C, " Root dir: ~p\n\n", [code:root_dir()]),
    cluster_info:format(C, " Lib dir: ~p\n\n", [code:lib_dir()]),
    %% io:format() output only {sniff!}
    %% cluster_info:format(C, " Clashes: ~p\n\n", [code:clash()]),
    PatchDirs = [Dir || Dir <- code:get_path(), string:str(Dir, "patch") /= 0],
    lists:foreach(
      fun(Dir) ->
              cluster_info:format(C, " Patch dir ~p:\n~s\n\n",
                                  [Dir, os:cmd("ls -l " ++ Dir)])
      end, PatchDirs),
    cluster_info:format(C, " All Paths:\n ~p\n\n", [code:get_path()]),
    [try
         cluster_info:format(C, " Module ~p:\n", [Mod]),
         cluster_info:format(C, " ~p\n\n", [Mod:module_info()])
     catch X:Y ->
             cluster_info:format(C, "Error for ~p: ~p ~p at ~p\n",
                                 [Mod, X, Y, erlang:get_stacktrace()])
     end || Mod <- lists:sort([M || {M, _} <- code:all_loaded()])].

memory_hogs(C, Num) ->
    L = lists:sort([{Mem, Pid}
                    || Pid <- processes(),
                       {memory, Mem} <- [catch process_info(Pid, memory)],
                       is_integer(Mem)]),
    cluster_info:format(C, " ~p\n", [lists:sublist(lists:reverse(L), Num)]).

nodes_info(C) ->
    cluster_info:format(C, " My node: ~p\n", [node()]),
    cluster_info:format(C, " Other nodes: ~p\n", [lists:sort(nodes())]).

non_zero_mailboxes(C) ->
    L = lists:sort([{Size, Pid}
                    || Pid <- processes(),
                       {message_queue_len, Size} <-
                           [catch process_info(Pid, message_queue_len)],
                       Size > 0]),
    cluster_info:format(C, " ~p\n", [lists:reverse(L)]).

port_info(C) ->
    L = [{Port, catch erlang:port_info(Port)} || Port <- erlang:ports()],
    cluster_info:format(C, " ~p\n", [L]).

registered_names(C) ->
    L = lists:sort([{Name, catch whereis(Name)} || Name <- registered()]),
    cluster_info:format(C, " ~p\n", [L]).

time_and_date(C) ->
    cluster_info:format(C, " Current date: ~p\n", [date()]),
    cluster_info:format(C, " Current time: ~p\n", [time()]),
    cluster_info:format(C, " Current now : ~p\n", [now()]).

timer_status(C) ->
    cluster_info:format(C, " ~p\n", [timer:get_status()]).

