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

%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_core_handoff_manager).
-behaviour(gen_server).

%% gen_server api
-export([start_link/0,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

%% exclusion api
-export([add_exclusion/2,
         get_exclusions/1,
         remove_exclusion/2
        ]).

%% handoff api
-export([add_outbound/4,
         add_inbound/1,
         status/0,
         set_concurrency/1,
         kill_handoffs/0
        ]).

-include("riak_core_handoff.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type mod()   :: atom().
-type index() :: integer().

-record(handoff_status,
        { modindex      :: {mod(),index()},
          node          :: atom(),
          direction     :: inbound | outbound,
          transport_pid :: pid(),
          timestamp     :: tuple(),
          status        :: any(),
          vnode_pid     :: pid() | undefined
        }).

-record(state,
        { excl,
          handoffs :: [#handoff_status{}]
        }).

%% this can be overridden with riak_core handoff_concurrency
-define(HANDOFF_CONCURRENCY,1).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, #state{excl=ordsets:new(), handoffs=[]}}.

%% handoff_manager API

add_outbound(Module,Idx,Node,VnodePid) ->
    gen_server:call(?MODULE,{add_outbound,Module,Idx,Node,VnodePid}).

add_inbound(SSLOpts) ->
    gen_server:call(?MODULE,{add_inbound,SSLOpts}).

status() ->
    gen_server:call(?MODULE,status).

set_concurrency(Limit) ->
    gen_server:call(?MODULE,{set_concurrency,Limit}).

kill_handoffs() ->
    set_concurrency(0).

add_exclusion(Module, Index) ->
    gen_server:cast(?MODULE, {add_exclusion, {Module, Index}}).

remove_exclusion(Module, Index) ->
    gen_server:cast(?MODULE, {del_exclusion, {Module, Index}}).

get_exclusions(Module) ->
    gen_server:call(?MODULE, {get_exclusions, Module}, infinity).

%% gen_server API

handle_call({get_exclusions, Module}, _From, State=#state{excl=Excl}) ->
    Reply =  [I || {M, I} <- ordsets:to_list(Excl), M =:= Module],
    {reply, {ok, Reply}, State};
handle_call({add_outbound,Mod,Idx,Node,Pid},_From,State=#state{handoffs=HS}) ->
    case send_handoff(Mod,Idx,Node,Pid,HS) of
        {ok,Handoff=#handoff_status{transport_pid=Sender}} ->
            {reply,{ok,Sender},State#state{handoffs=HS ++ [Handoff]}};
        {false,_ExistingHandoff=#handoff_status{transport_pid=Sender}} ->
            {reply,{ok,Sender},State};
        Error ->
            {reply,Error,State}
    end;
handle_call({add_inbound,SSLOpts},_From,State=#state{handoffs=HS}) ->
    case receive_handoff(SSLOpts) of
        {ok,Handoff=#handoff_status{transport_pid=Receiver}} ->
            {reply,{ok,Receiver},State#state{handoffs=HS ++ [Handoff]}};
        Error ->
            {reply,Error,State}
    end;
handle_call(status,_From,State=#state{handoffs=HS}) ->
    Handoffs=[{M,N,D,active,S} ||
                 #handoff_status{modindex=M,node=N,direction=D,status=S} <- HS],
    {reply, Handoffs, State};
handle_call({set_concurrency,Limit},_From,State=#state{handoffs=HS}) ->
    application:set_env(riak_core,handoff_concurrency,Limit),
    case Limit < erlang:length(HS) of
        true ->
            %% Note: we don't update the state with the handoffs that we're
            %% keeping because we'll still get the 'DOWN' messages with
            %% a reason of 'max_concurrency' and we want to be able to do
            %% something with that if necessary.
            {_Keep,Discard}=lists:split(Limit,HS),
            [erlang:exit(Pid,max_concurrency) ||
                #handoff_status{transport_pid=Pid} <- Discard],
            {reply, ok, State};
        false ->
            {reply, ok, State}
    end.


handle_cast({del_exclusion, {Mod, Idx}}, State=#state{excl=Excl}) ->
    {noreply, State#state{excl=ordsets:del_element({Mod, Idx}, Excl)}};
handle_cast({add_exclusion, {Mod, Idx}}, State=#state{excl=Excl}) ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    case riak_core_ring:my_indices(Ring) of
        [] ->
            %% Trigger a ring update to ensure the node shuts down
            riak_core_ring_events:ring_update(Ring);
        _ ->
            ok
    end,
    {noreply, State#state{excl=ordsets:add_element({Mod, Idx}, Excl)}}.


handle_info({'DOWN',_Ref,process,Pid,Reason},State=#state{handoffs=HS}) ->
    case lists:keytake(Pid,#handoff_status.transport_pid,HS) of
        {value,
         #handoff_status{modindex={M,I},direction=Dir,vnode_pid=Vnode},
         NewHS
        } ->
            WarnVnode =
                case Reason of
                    %% if the reason the handoff process died was anything other
                    %% than 'normal' we should log the reason why as an error
                    normal ->
                        false;
                    max_concurrency ->
                        lager:info("An ~w handoff of partition ~w ~w was terminated for reason: ~w~n", [Dir,M,I,Reason]),
                        true;
                    _ ->
                        lager:error("An ~w handoff of partition ~w ~w was terminated for reason: ~w~n", [Dir,M,I,Reason]),
                        true
                end,

            %% if we have the vnode process pid, tell the vnode why the
            %% handoff stopped so it can clean up its state
            case WarnVnode andalso is_pid(Vnode) of
                true ->
                    riak_core_vnode:handoff_error(Vnode,'DOWN',Reason);
                _ ->
                    ok
            end,

            %% removed the handoff from the list of active handoffs
            {noreply, State#state{handoffs=NewHS}};
        false ->
            {noreply, State}
    end;
handle_info(Info, State) ->
    io:format(">>>>> ~w~n", [Info]),
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%
%% @private functions
%%

get_concurrency_limit () ->
    app_helper:get_env(riak_core,handoff_concurrency,?HANDOFF_CONCURRENCY).

%% true if handoff_concurrency (inbound + outbound) hasn't yet been reached
handoff_concurrency_limit_reached () ->
    Receivers=supervisor:count_children(riak_core_handoff_receiver_sup),
    Senders=supervisor:count_children(riak_core_handoff_sender_sup),
    ActiveReceivers=proplists:get_value(active,Receivers),
    ActiveSenders=proplists:get_value(active,Senders),
    get_concurrency_limit() =< (ActiveReceivers + ActiveSenders).

%% spawn a sender process
send_handoff (Mod,Idx,Node,Vnode,HS) ->
    case handoff_concurrency_limit_reached() of
        true ->
            {error, max_concurrency};
        false ->
            ShouldHandoff=
                case lists:keyfind({Mod,Idx},#handoff_status.modindex,HS) of
                    false ->
                        true;
                    Handoff=#handoff_status{node=Node,vnode_pid=Vnode} ->
                        {false,Handoff};
                    #handoff_status{transport_pid=Sender} ->
                        %% found a running handoff with a different vnode
                        %% source or a different arget ndoe, kill the current
                        %% one and the new one will start up
                        erlang:exit(Sender,resubmit_handoff_change),
                        true
                end,

            case ShouldHandoff of
                true ->
                    %% start the sender process
                    {ok,Pid}=riak_core_handoff_sender_sup:start_sender(Node,
                                                                       Mod,
                                                                       Idx,
                                                                       Vnode),
                    erlang:monitor(process,Pid),

                    %% successfully started up a new sender handoff
                    {ok, #handoff_status{ transport_pid=Pid,
                                          direction=outbound,
                                          timestamp=now(),
                                          node=Node,
                                          modindex={Mod,Idx},
                                          vnode_pid=Vnode,
                                          status=[]
                                        }
                    };

                %% handoff already going, just return it
                AlreadyExists={false,_CurrentHandoff} ->
                    AlreadyExists
            end
    end.

%% spawn a receiver process
receive_handoff (SSLOpts) ->
    case handoff_concurrency_limit_reached() of
        true ->
            {error, max_concurrency};
        false ->
            {ok,Pid}=riak_core_handoff_receiver_sup:start_receiver(SSLOpts),
            erlang:monitor(process,Pid),

            %% successfully started up a new receiver
            {ok, #handoff_status{ transport_pid=Pid,
                                  direction=inbound,
                                  timestamp=now(),
                                  modindex={undefined,undefined},
                                  node=undefined,
                                  status=[]
                                }
            }
    end.

%%
%% EUNIT tests...
%%

-ifdef (TEST_BROKEN_AZ_TICKET_1042).

handoff_test_ () ->
    {spawn,
     {setup,

      %% called when the tests start and complete...
      fun () -> {ok,Pid}=start_link(), Pid end,
      fun (Pid) -> exit(Pid,kill) end,

      %% actual list of test
      [?_test(simple_handoff())
      ]}}.

simple_handoff () ->
    ?assertEqual([],status()),

    %% clear handoff_concurrency and make sure a handoff fails
    ?assertEqual(ok,set_concurrency(0)),
    ?assertEqual({error,max_concurrency},add_inbound([])),
    ?assertEqual({error,max_concurrency},add_outbound(riak_kv,0,node(),self())),

    %% allow for a single handoff
    ?assertEqual(ok,set_concurrency(1)),

    %% done
    ok.

-endif.
