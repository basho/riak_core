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
         unregister_vnode/3, command_return_vnode/2, overloaded/1]).
-export([system_continue/3, system_terminate/4, system_code_change/4]).

-include("riak_core_vnode.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {mod                    :: atom(),
                index                  :: partition(),
                vnode_pid              :: pid(),
                vnode_mref             :: reference(),
                check_mailbox          :: non_neg_integer(),
                check_threshold        :: pos_integer() | undefined,
                check_counter          :: non_neg_integer(),
                check_interval         :: pos_integer(),
                check_request_interval :: non_neg_integer(),
                check_request          :: undefined | sent | ignore
               }).

-define(DEFAULT_CHECK_INTERVAL, 5000).
-define(DEFAULT_OVERLOAD_THRESHOLD, 10000).

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

    Interval = app_helper:get_env(riak_core,
                                  vnode_check_interval,
                                  ?DEFAULT_CHECK_INTERVAL),
    RequestInterval = app_helper:get_env(riak_core,
                                         vnode_check_request_interval,
                                         Interval div 2),
    Threshold = app_helper:get_env(riak_core,
                                   vnode_overload_threshold,
                                   ?DEFAULT_OVERLOAD_THRESHOLD),

    SafeInterval =
        case (Threshold == undefined) orelse (Interval < Threshold) of
            true ->
                Interval;
            false ->
                lager:warning("Setting riak_core/vnode_check_interval to ~b",
                              [Threshold div 2]),
                Threshold div 2
        end,
    SafeRequestInterval =
        case RequestInterval < SafeInterval of
            true ->
                RequestInterval;
            false ->
                lager:warning("Setting riak_core/vnode_check_request_interval "
                              "to ~b", [SafeInterval div 2]),
                SafeInterval div 2
        end,

    State = #state{mod=Mod,
                   index=Index,
                   check_mailbox=0,
                   check_counter=0,
                   check_threshold=Threshold,
                   check_interval=SafeInterval,
                   check_request_interval=SafeRequestInterval},
    loop(Parent, State).

unregister_vnode(Mod, Index, Pid) ->
    cast(reg_name(Mod, Index), {unregister_vnode, Pid}).

-spec command_return_vnode({atom(), non_neg_integer(), atom()}, term()) ->
                                  {ok, pid()} | {error, term()}.
command_return_vnode({Mod,Index,Node}, Req) ->
    call(reg_name(Mod, Index, Node), {return_vnode, Req}).

%% Return true if the next proxied message will return overload
overloaded({Mod, Index, Node}) ->
    call(reg_name(Mod, Index, Node), overloaded);
overloaded(Pid) ->
    call(Pid, overloaded).

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
            {_, Reply} = gen:reply(From, Reply),
            loop(Parent, NewState);
        {'$vnode_proxy_cast', Msg} ->
            {noreply, NewState} = handle_cast(Msg, State),
            loop(Parent, NewState);
        {'DOWN', _Mref, process, _Pid, _} ->
            NewState = forget_vnode(State),
            loop(Parent, NewState);
        {system, From, Msg} ->
            sys:handle_system_msg(Msg, From, Parent, ?MODULE, [], State);
        Msg ->
            {noreply, NewState} = handle_proxy(Msg, State),
            loop(Parent, NewState)
    end.

%% @private
handle_call({return_vnode, Req}, _From, State) ->
    {Pid, NewState} = get_vnode_pid(State),
    gen_fsm:send_event(Pid, Req),
    {reply, {ok, Pid}, NewState};
handle_call(overloaded, _From, State=#state{check_mailbox=Mailbox,
                                            check_threshold=Threshold}) ->
    Result = (Mailbox > Threshold),
    {reply, Result, State};
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

%% @private
handle_cast({unregister_vnode, Pid}, State) ->
    %% The pid may not match the vnode_pid in the state, but we must send the
    %% unregister event anyway -- the vnode manager requires it.
    gen_fsm:send_event(Pid, unregistered),
    catch demonitor(State#state.vnode_mref, [flush]),
    NewState = forget_vnode(State),
    {noreply, NewState};
handle_cast({vnode_proxy_pong, Ref, Msgs}, State=#state{check_request=RequestState,
                                                        check_mailbox=Mailbox}) ->
    NewState = case Ref of
                   RequestState ->
                       State#state{check_mailbox=Mailbox - Msgs,
                                   check_request=undefined,
                                   check_counter=0};
                   _ ->
                       State
               end,
    {noreply, NewState};

handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_proxy(Msg, State=#state{check_threshold=undefined}) ->
    {Pid, NewState} = get_vnode_pid(State),
    Pid ! Msg,
    {noreply, NewState};
handle_proxy(Msg, State=#state{check_counter=Counter,
                               check_mailbox=Mailbox,
                               check_interval=Interval,
                               check_request_interval=RequestInterval,
                               check_request=RequestState,
                               check_threshold=Threshold}) ->
    %%
    %% NOTE: This function is intentionally written as it is for performance
    %%       reasons -- the vnode proxy is on the critical path of Riak and
    %%       must be fast enough to shed already queued work even under
    %%       extreme overload conditions. Things that are intentional: the
    %%       use of a monolithic function rather than separate smaller
    %%       functions; exporting values from case statements rather than
    %%       generating + pattern matching on intermediary tuples; and
    %%       updating State once at the very end of the function to ensure
    %%       that State is only copied once rather than multiple times.
    %%
    %%       When changing this function, please run:
    %%         erts_debug:df(riak_core_vnode_proxy)
    %%       and look over the generated riak_core_vnode_proxy.dis file to
    %%       ensure unnecessary work is not being performed needlessly.
    %%
    case State#state.vnode_pid of
        undefined ->
            {Pid, State2} = get_vnode_pid(State);
        KnownPid ->
            Pid = KnownPid,
            State2 = State
    end,

    Mailbox2 = case Mailbox =< Threshold of
                   true ->
                       Pid ! Msg,
                       Mailbox + 1;
                   false ->
                       handle_overload(Msg, State),
                       Mailbox
               end,

    Counter2 = Counter + 1,
    case Counter2 of
        RequestInterval ->
            %% Ping the vnode in hopes that we get a pong back before hitting
            %% the hard query interval and triggering an expensive process_info
            %% call. A successful pong from the vnode means that all messages
            %% sent before the ping have already been handled and therefore
            %% we can adjust our mailbox estimate accordingly.
            case RequestState of
                undefined ->
                    RequestState2 = send_proxy_ping(Pid, Mailbox2);
                _ ->
                    RequestState2 = RequestState
            end,
            Mailbox3 = Mailbox2,
            Counter3 = Counter2;
        Interval ->
            %% Time to directly check the mailbox size. This operation may
            %% be extremely expensive. If the vnode is currently active,
            %% the proxy will be descheduled until the vnode finishes
            %% execution and becomes descheduled itself.
            {_, L} =
                erlang:process_info(Pid, message_queue_len),
            Counter3 = 0,
            Mailbox3 = L + 1,
            %% Send a new proxy ping so that if the new length is above the
            %% threshold then the proxy will detect the work is completed,
            %% rather than being stuck in overload state until the interval
            %% counts are reached.
            RequestState2 = send_proxy_ping(Pid, Mailbox3);
        _ ->
            Mailbox3 = Mailbox2,
            Counter3 = Counter2,
            RequestState2 = RequestState
    end,
    {noreply, State2#state{check_counter=Counter3,
                           check_mailbox=Mailbox3,
                           check_request=RequestState2}}.

handle_overload(Msg, #state{mod=Mod, index=Index}) ->
    riak_core_stat:update(dropped_vnode_requests),
    case Msg of
        {'$gen_event', ?VNODE_REQ{sender=Sender, request=Request}} ->
            catch(Mod:handle_overload_command(Request, Sender, Index));
        {'$gen_event', ?COVERAGE_REQ{sender=Sender, request=Request}} ->
            catch(Mod:handle_overload_command(Request, Sender, Index));
        _ ->
            catch(Mod:handle_overload_info(Msg, Index))
    end.

%% @private
forget_vnode(State) ->
    State#state{vnode_pid=undefined,
                vnode_mref=undefined,
                check_mailbox=0,
                check_counter=0,
                check_request=undefined}.

%% @private
get_vnode_pid(State=#state{mod=Mod, index=Index, vnode_pid=undefined}) ->
    {ok, Pid} = riak_core_vnode_manager:get_vnode_pid(Index, Mod),
    Mref = erlang:monitor(process, Pid),
    NewState = State#state{vnode_pid=Pid, vnode_mref=Mref},
    {Pid, NewState};
get_vnode_pid(State=#state{vnode_pid=Pid}) ->
    {Pid, State}.

%% @private
send_proxy_ping(Pid, MailboxSizeAfterPing) ->
    Ref = make_ref(),
    Pid ! {'$vnode_proxy_ping', self(), Ref, MailboxSizeAfterPing},
    Ref.

-ifdef(TEST).

update_msg_counter() ->
    Count = case erlang:get(count) of
        undefined -> 0;
        Val -> Val
    end,
    put(count, Count+1).

fake_loop() ->
    receive
        block ->
            fake_loop_block();
        slow ->
            fake_loop_slow();
        {get_count, Pid} ->
            Pid ! {count, erlang:get(count)},
            fake_loop();
        %% Original tests do not expect replies - the
        %% results below expect the pings to be counted
        %% towards messages received.  If you ever wanted
        %% to re-instance, uncomment below.
        %% {'$vnode_proxy_ping', ReplyTo, Ref, Msgs} ->
        %%     ReplyTo ! {Ref, Msgs},
        %%     fake_loop();
        _Msg ->
            update_msg_counter(),
            fake_loop()
    end.

fake_loop_slow() ->
    timer:sleep(100),
    receive
        _Msg ->
            update_msg_counter(),
            fake_loop_slow()
    end.

fake_loop_block() ->
    receive
        unblock ->
            fake_loop()
    end.

overload_test_() ->
    {timeout, 900, {foreach,
     fun() ->
             VnodePid = spawn(fun fake_loop/0),
             meck:unload(),
             meck:new(riak_core_vnode_manager, [passthrough]),
             meck:expect(riak_core_vnode_manager, get_vnode_pid,
                         fun(_Index, fakemod) -> {ok, VnodePid};
                            (Index, Mod) -> meck:passthrough([Index, Mod])
                         end),
             meck:new(fakemod, [non_strict]),
             meck:expect(fakemod, handle_overload_info, fun(hello, _Idx) ->
                                                            ok
                                                        end),

             {ok, ProxyPid} = riak_core_vnode_proxy:start_link(fakemod, 0),
             unlink(ProxyPid),
             {VnodePid, ProxyPid}
     end,
     fun({VnodePid, ProxyPid}) ->
             unlink(VnodePid),
             unlink(ProxyPid),
             exit(VnodePid, kill),
             exit(ProxyPid, kill)
     end,
     [
      fun({_VnodePid, ProxyPid}) ->
              {"should not discard in normal operation", timeout, 60,
               fun() ->
                       ToSend = ?DEFAULT_OVERLOAD_THRESHOLD,
                       [ProxyPid ! hello || _ <- lists:seq(1, ToSend)],

                       %% synchronize on the proxy and the mailbox
                       {ok, ok} = gen:call(ProxyPid, '$vnode_proxy_call', sync, infinity),
                       ProxyPid ! {get_count, self()},
                       receive
                           {count, Count} ->
                               %% First will hit the request check interval,
                               %% then will check message queue every interval
                               %% (no new ping will be resubmitted after the first
                               %% as the request will already have a reference)
                               PingReqs = 1 + % for first request intarval
                                   ToSend div ?DEFAULT_CHECK_INTERVAL,
                               ?assertEqual(ToSend+PingReqs, Count)
                       end
               end
              }
      end,
      fun({VnodePid, ProxyPid}) ->
              {"should discard during overflow", timeout, 60,
               fun() ->
                       VnodePid ! block,
                       [ProxyPid ! hello || _ <- lists:seq(1, 50000)],
                       %% synchronize on the mailbox - no-op that hits msg catchall
                       Reply = gen:call(ProxyPid, '$vnode_proxy_call', sync, infinity),
                       ?assertEqual({ok, ok}, Reply),
                       VnodePid ! unblock,
                       VnodePid ! {get_count, self()},
                       receive
                           {count, Count} ->
                               %% Threshold + 10 unanswered vnode_proxy_ping
                               ?assertEqual(?DEFAULT_OVERLOAD_THRESHOLD + 10, Count)
                       end
               end
              }
      end,
      fun({VnodePid, ProxyPid}) ->
              {"should tolerate slow vnodes", timeout, 60,
               fun() ->
                       VnodePid ! slow,
                       [ProxyPid ! hello || _ <- lists:seq(1, 50000)],
                       %% synchronize on the mailbox - no-op that hits msg catchall
                       Reply = gen:call(ProxyPid, '$vnode_proxy_call', sync, infinity),
                       ?assertEqual({ok, ok}, Reply),
                       %% check that the outstanding message count is
                       %% reasonable
                       {message_queue_len, L} =
                           erlang:process_info(VnodePid, message_queue_len),
                       %% Threshold + 2 unanswered vnode_proxy_ping (one
                       %% for first ping, second after process_info check)
                       ?assert(L =< (?DEFAULT_OVERLOAD_THRESHOLD + 2))
               end
              }
      end
     ]}}.
-endif.
