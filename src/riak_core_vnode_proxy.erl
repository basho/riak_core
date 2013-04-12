%% -------------------------------------------------------------------
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_core_vnode_proxy).
-export([start_link/2, init/1, reg_name/2, reg_name/3, call/2, call/3, cast/2,
         unregister_vnode/3, command_return_vnode/2]).
-export([system_continue/3, system_terminate/4, system_code_change/4]).

-record(state, {mod, index, vnode_pid, vnode_mref, check_interval,
                check_threshold, check_counter=0, check_mailbox=0}).

-include("riak_core_vnode.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

reg_name(Mod, Index) ->
    ModBin = atom_to_binary(Mod, latin1),
    IdxBin = list_to_binary(integer_to_list(Index)),
    AllBin = <<$p,$r,$o,$x,$y,$_, ModBin/binary, $_, IdxBin/binary>>,
    binary_to_atom(AllBin, latin1).

reg_name(Mod, Index, Node) ->
    {reg_name(Mod, Index), Node}.

start_link(Mod, Index) ->
    RegName = reg_name(Mod, Index),
    proc_lib:start_link(?MODULE, init, [[self(), RegName, Mod, Index]]).

init([Parent, RegName, Mod, Index]) ->
    erlang:register(RegName, self()),
    proc_lib:init_ack(Parent, {ok, self()}),
    State = #state{mod=Mod, index=Index, check_interval=5000,
                   check_threshold=10000},
    loop(Parent, State).

unregister_vnode(Mod, Index, Pid) ->
    cast(reg_name(Mod, Index), {unregister_vnode, Pid}).

-spec command_return_vnode({atom(), non_neg_integer(), atom()}, term()) ->
                                  {ok, pid()} | {error, term()}.
command_return_vnode({Mod,Index,Node}, Req) ->
    call(reg_name(Mod, Index, Node), {return_vnode, Req}).

call(Name, Msg) ->
    call_reply(catch gen:call(Name, '$vnode_proxy_call', Msg)).

call(Name, Msg, Timeout) ->
    call_reply(catch gen:call(Name, '$vnode_proxy_call', Msg, Timeout)).

-spec call_reply({atom(), term()}) -> term().
call_reply({ok, Res}) ->
    Res;
call_reply({'EXIT', Reason}) ->
    {error, Reason}.

cast(Name, Msg) ->
    catch erlang:send(Name, {'$vnode_proxy_cast', Msg}),
    ok.

system_continue(Parent, _, State) ->
    loop(Parent, State).

system_terminate(Reason, _Parent, _, _State) ->
    exit(Reason).

system_code_change(State, _, _, _) ->
    {ok, State}.

%% @private
loop(Parent, State) ->
    receive
        {'$vnode_proxy_call', From, Msg} ->
            {reply, Reply, NewState} = handle_call(Msg, From, State),
            gen:reply(From, Reply),
            loop(Parent, NewState);
        {'$vnode_proxy_cast', Msg} ->
            {noreply, NewState} = handle_cast(Msg, State),
            loop(Parent, NewState);
        {'DOWN', _Mref, process, _Pid, _} ->
            NewState = State#state{vnode_pid=undefined, vnode_mref=undefined},
            loop(Parent, NewState);
        {system, From, Msg} ->
            sys:handle_system_msg(Msg, From, Parent, ?MODULE, [], State);
        Msg ->
            {noreply, NewState} = handle_proxy(Msg,
                                               State#state{check_counter=State#state.check_counter+1,
                                                           check_mailbox=State#state.check_mailbox+1}),
            loop(Parent, NewState)
    end.

%% @private
handle_call({return_vnode, Req}, _From, State) ->
    {Pid, NewState} = get_vnode_pid(State),
    gen_fsm:send_event(Pid, Req),
    {reply, {ok, Pid}, NewState};

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

%% @private
handle_cast({unregister_vnode, Pid}, State) ->
    %% The pid may not match the vnode_pid in the state, but we must send the
    %% unregister event anyway -- the vnode manager requires it.
    gen_fsm:send_event(Pid, unregistered),
    catch demonitor(State#state.vnode_mref, [flush]),
    NewState = State#state{vnode_pid=undefined, vnode_mref=undefined},
    {noreply, NewState};
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_proxy(Msg, State) ->
    #state{check_counter=Counter, check_interval=Interval,
           check_threshold=Threshold, check_mailbox=Mailbox0, mod=Mod} = State,
    {Pid, NewState} = get_vnode_pid(State),
    {Mailbox, NewCounter} = case Counter >= Interval andalso (Counter rem Interval) == 0 of
                                true ->
                                    %% time to check the mailbox size
                                    {message_queue_len, L} =
                                        erlang:process_info(Pid, message_queue_len),
                                    lager:debug("reset mailbox to ~p", [L]),
                                    {L, 0};
                                false ->
                                    {Mailbox0, Counter}
                            end,

    case Mailbox < Threshold of
        true ->
            Pid ! Msg;
        false ->
            case Msg of
                {'$gen_event', ?VNODE_REQ{sender=Sender, request=Request}} ->
                    catch(Mod:handle_overload_command(Request, Sender,
                                                      State#state.index));
                _ ->
                    ok
            end
    end,
    {noreply, NewState#state{check_mailbox=Mailbox, check_counter=NewCounter}}.

%% @private
get_vnode_pid(State=#state{mod=Mod, index=Index, vnode_pid=undefined}) ->
    {ok, Pid} = riak_core_vnode_manager:get_vnode_pid(Index, Mod),
    Mref = erlang:monitor(process, Pid),
    NewState = State#state{vnode_pid=Pid, vnode_mref=Mref},
    {Pid, NewState};
get_vnode_pid(State=#state{vnode_pid=Pid}) ->
    {Pid, State}.

-ifdef(TEST).

fake_loop() ->
    receive
        block ->
            fake_loop_block();
        slow ->
            fake_loop_slow();
        {get_count, Pid} ->
            Pid ! {count, erlang:get(count)},
            fake_loop();
        _Msg ->
            Count = case erlang:get(count) of
                        undefined -> 0;
                        Val -> Val
                    end,
            put(count, Count+1),
            fake_loop()
    end.

fake_loop_slow() ->
    timer:sleep(100),
    receive
        _Msg ->
            Count = case erlang:get(count) of
                        undefined -> 0;
                        Val -> Val
                    end,
            put(count, Count+1),
            fake_loop_slow()
    end.

fake_loop_block() ->
    receive
        unblock ->
            fake_loop()
    end.

overload_test_() ->
    {foreach,
     fun() ->
             VnodePid = spawn(fun fake_loop/0),
             meck:new(riak_core_vnode_manager, [passthrough]),
             meck:expect(riak_core_vnode_manager, get_vnode_pid,
                         fun(_Index, fakemod) -> {ok, VnodePid};
                            (Index, Mod) -> meck:passthrough([Index, Mod])
                         end),
             {ok, ProxyPid} = riak_core_vnode_proxy:start_link(fakemod, 0),
             unlink(ProxyPid),
             {VnodePid, ProxyPid}
     end,
     fun({VnodePid, ProxyPid}) ->
             meck:unload(riak_core_vnode_manager),
             exit(VnodePid, kill),
             exit(ProxyPid, kill)
     end,
     [
      fun({VnodePid, ProxyPid}) ->
              {"should not discard in normal operation",
               fun() ->
                       [ProxyPid ! hello || _ <- lists:seq(1, 50000)],
                       %% synchronize on the mailbox
                       Reply = gen:call(ProxyPid, '$vnode_proxy_call', sync),
                       ?assertEqual({ok, ok}, Reply),
                       VnodePid ! {get_count, self()},
                       receive
                           {count, Count} ->
                               ?assertEqual(50000, Count)
                       end
               end
              }
      end,
      fun({VnodePid, ProxyPid}) ->
              {"should discard during overflow",
               fun() ->
                       VnodePid ! block,
                       [ProxyPid ! hello || _ <- lists:seq(1, 50000)],
                       %% synchronize on the mailbox
                       Reply = gen:call(ProxyPid, '$vnode_proxy_call', sync),
                       ?assertEqual({ok, ok}, Reply),
                       VnodePid ! unblock,
                       VnodePid ! {get_count, self()},
                       receive
                           {count, Count} ->
                               ?assertEqual(10000, Count)
                       end
               end
              }
      end,
      fun({VnodePid, ProxyPid}) ->
              {"should tolerate slow vnodes",
               fun() ->
                       VnodePid ! slow,
                       [ProxyPid ! hello || _ <- lists:seq(1, 50000)],
                       %% synchronize on the mailbox
                       Reply = gen:call(ProxyPid, '$vnode_proxy_call', sync),
                       ?assertEqual({ok, ok}, Reply),
                       %% check that the outstanding message count is
                       %% reasonable
                       {message_queue_len, L} =
                           erlang:process_info(VnodePid, message_queue_len),
                       ?assert(L =< 10000)
               end
              }
      end
     ]}.
-endif.
