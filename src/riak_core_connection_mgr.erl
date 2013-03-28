%% -------------------------------------------------------------------
%%
%% Riak  Subprotocol Server Dispatch and Client Connections
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_core_connection_mgr).
-author("Chris Tilt").
-behaviour(gen_server).

-include("riak_core_connection.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% controls retry and backoff.
-define(INITIAL_BACKOFF, 1 * 1000).  %% 1 second initial backoff per endpoint
-define(MAX_BACKOFF, 1 * 60 * 1000). %% 1 minute maximum backoff per endpoint
-define(EXHAUSTED_ENDPOINTS_RETRY_INTERVAL, 10 * 1000). %% 10 seconds until retrying the list again

%% retry delay if locator returned empty list
-ifdef(TEST).
-define(TRACE(Stmt),Stmt).
%%-define(TRACE(Stmt),ok).
-define(DEFAULT_RETRY_NO_ENDPOINTS, 2 * 1000). %% short for testing to avoid timeout
-else.
-define(TRACE(Stmt),ok).
-define(DEFAULT_RETRY_NO_ENDPOINTS, 5 * 1000). %% 5 seconds
-endif.


-define(SERVER, riak_core_connection_manager).
-define(MAX_LISTENERS, 100).

-type(counter() :: non_neg_integer()).

%% Connection manager strategy (per Jon M.)
%% when a connection request comes in,
%% + call the locator service to get the list of {transport, {address, port}}
%% + create a linked helper process to call riak_core_connection (just once) on the next available
%%   connection (ignore blacklisted ones, they'll get picked up if a repeat is necessary)
%% + on connection it transfers control of the socket back to the connmgr, casts a success message back
%%   to the connection manager and exits normally.
%%   - on success, the connection manager increments successful connects, reset the backoff timeout on
%%     that connection.
%%   - on failure, casts a failure message back to the connection manager (error, timeout etc) the
%%     connection manager marks the {Transport, {Address, Port}} as blacklisted, increases the failure
%%     counter and starts a timer for the backoff time (and updates it for next time). The connection
%%     manager checks for the next non--blacklisted endpoint in the connection request list to launch
%%     a new connection, if the list is empty call the locator service again to get a new list. If all
%%     connections are blacklisted, use send_after message to wake up and retry (perhaps with backoff
%%     time too).

%% End-point status state, updated for failed and successful connection attempts,
%% or by timers that fire to update the backoff time.
%% TODO: add folsom window'd stats
%% handle an EXIT from the helper process if it dies
-record(ep, {addr,                                 %% endpoint {IP, Port}
             nb_curr_connections = 0 :: counter(), %% number of current connections
             nb_success = 0 :: counter(),   %% total successfull connects on this ep
             nb_failures = 0 :: counter(),  %% total failed connects on this ep
             is_black_listed = false :: boolean(), %% true after a failed connection attempt
             backoff_delay=0 :: counter(),  %% incremented on each failure, reset to zero on success
             failures = orddict:new() :: orddict:orddict(), %% failure reasons
             last_fail_time :: erlang:timestamp(),          %% time of last failure since 1970
             next_try_secs :: counter()     %% time in seconds to next retry attempt
             }).

%% connection request record
-record(req, {ref,      % Unique reference for this connection request
              pid,      % Helper pid trying to make connection
              target,   % target to connect to {Type, Name}
              spec,     % client spec
              strategy, % connection strategy
              cur,      % current connection endpoint
              state = init,  % init | connecting | connected | cancelled
              status    % history of connection attempts
             }).
              

%% connection manager state:
-record(state, {is_paused = false :: boolean(),
                pending = [] :: [#req{}], % pending requests
                %% endpoints :: {module(),ip_addr()} -> ep()
                endpoints = orddict:new() :: orddict:orddict(), %% known endpoints w/status
                locators = orddict:new() :: orddict:orddict(), %% connection locators
                nb_total_succeeded = 0 :: counter(),
                nb_total_failed = 0 :: counter()
               }).

-export([start_link/0,
         resume/0,
         pause/0,
         is_paused/0,
         connect/2, connect/3,
         disconnect/1,
         register_locator/2,
         apply_locator/2,
         reset_backoff/0,
         stop/0
         ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% internal functions
-export([connection_helper/4, increase_backoff/1]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Starts the manager linked.
-spec(start_link() -> {ok, pid()}).
start_link() ->
    Args = [],
    Options = [],
    gen_server:start_link({local, ?SERVER}, ?MODULE, Args, Options).

%% @doc Begins or resumes accepting and establishing new connections, in
%% order to maintain the protocols that have been (or continue to be) registered
%% and unregistered. pause() will not kill any existing connections, but will
%% cease accepting new requests or retrying lost connections.
-spec(resume() -> ok).
resume() ->
    gen_server:cast(?SERVER, resume).

%% @doc Stop accepting / creating new connections; this does not terminated
%% existing ones.
-spec(pause() -> ok).
pause() ->
    gen_server:cast(?SERVER, pause).

%% @doc Return paused state
-spec is_paused() -> boolean().
is_paused() ->
    gen_server:call(?SERVER, is_paused).

%% @doc Reset all backoff delays to zero.
-spec reset_backoff() -> 'ok'.
reset_backoff() ->
    gen_server:cast(?SERVER, reset_backoff).

%% Register a locator - for the given Name and strategy it returns {ok, [{IP,Port}]}
%% list of endpoints to connect to, in order. The list may be empty.  
%% If the query can never be answered
%% return {error, Reason}.
%% fun(Name
register_locator(Type, Fun) ->
    gen_server:call(?SERVER, {register_locator, Type, Fun}, infinity).

apply_locator(Name, Strategy) ->
    gen_server:call(?SERVER, {apply_locator, Name, Strategy}, infinity).

%% @doc Establish a connection to the remote destination. be persistent about it,
%% but not too annoying to the remote end. Connect by name of cluster or
%% IP address. Use default strategy to find "best" peer for connection.
%%
%% Targets are found by applying a registered locator for it.
%% The identity locator is pre-installed, so if you want to connect to a list
%% of IP and Port addresses, supply a Target like this: {identity, [{IP, Port},...]},
%% where IP::string() and Port::integer(). You can also pass {identity, {IP, Port}}
%% and the locator will use just that one IP. With a list, it will rotate
%% trying them all until a connection is established.
%%
%% Other locator types must be registered with this connection manager
%% before calling connect().
%%
%% Supervision must be done by the calling process if desired. No supervision
%% is done here.
%%
-spec connect(Target :: string(), ClientSpec :: clientspec(), Strategy :: client_scheduler_strategy()) -> {'ok', reference()}.
connect(Target, ClientSpec, Strategy) ->
    gen_server:call(?SERVER, {connect, Target, ClientSpec, Strategy}).

%% @doc same as connect(Target, ClientSpec, default).
%% @see connect/3
-spec connect(Target :: string(), ClientSpec :: clientspec()) -> {'ok', reference()}.
connect(Target, ClientSpec) ->
    gen_server:call(?SERVER, {connect, Target, ClientSpec, default}).

%% @doc Disconnect from the remote side.
-spec disconnect(Target :: string()) -> 'ok'.
disconnect(Target) ->
    gen_server:cast(?SERVER, {disconnect, Target}).

%% doc Stop the server and sever all connections.
-spec stop() -> 'ok'.
stop() ->
    gen_server:call(?SERVER, stop).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    process_flag(trap_exit, true),
    %% install default "identity" locator
    Locator = fun identity_locator/2,
    ?TRACE(?debugMsg("Starting")),
    {ok, #state{is_paused = false,
                locators = orddict:store(identity, Locator, orddict:new())
               }}.

handle_call(is_paused, _From, State) ->
    {reply, State#state.is_paused, State};

%% connect based on address. Return process id of helper
handle_call({connect, Target, ClientSpec, Strategy}, _From, State) ->
    Reference = make_ref(),
    Request = #req{ref = Reference,
                   target = Target,
                   pid = undefined,
                   spec = ClientSpec,
                   state = init,
                   strategy = Strategy},
    %% add request to pending queue so it may be found in restarts
    State2 = State#state{pending = lists:keystore(Reference, #req.ref,
                                                  State#state.pending,
                                                  Request)},
    ?TRACE(?debugFmt("Starting connect request to ~p, ref is ~p", [Target, Reference])),
    {reply, {ok, Reference}, start_request(Request, State2)};

handle_call({get_endpoint_backoff, Addr}, _From, State) ->
    ?TRACE(?debugFmt("backing off ~p", [Addr])),
    {reply, {ok, get_endpoint_backoff(Addr, State#state.endpoints)}, State};

handle_call({register_locator, Type, Fun}, _From,
            State = #state{locators = Locators}) ->
    {reply, ok, State#state{locators = orddict:store(Type, Fun, Locators)}};

handle_call({apply_locator, Target, Strategy}, _From,
            State = #state{locators = Locators}) ->
    AddrsOrError = locate_endpoints(Target, Strategy, Locators),
    {reply, AddrsOrError, State};

handle_call(stop, _From, State) ->
    %% TODO do we need to cleanup helper pids here?
    {stop, normal, ok, State};

handle_call({should_try_endpoint, Ref, Addr}, _From, State = #state{pending=Pending}) ->
    case lists:keyfind(Ref, #req.ref, Pending) of
        false ->
            %% This should never happen
            {reply, false, State};
        Req ->
            {Answer, ReqState} =
                case Req#req.state of
                    cancelled ->
                        %% helper process hasn't cancelled itself yet.
                        {false, cancelled};
                    _ ->
                        {true, connecting}
                end,
            {reply, Answer, State#state{pending = lists:keystore(Ref, #req.ref, Pending,
                                                                 Req#req{cur = Addr,
                                                                         state = ReqState})}}
    end;

handle_call(_Unhandled, _From, State) ->
    ?TRACE(?debugFmt("Unhandled gen_server call: ~p", [_Unhandled])),
    {reply, {error, unhandled}, State}.

handle_cast(pause, State) ->
    {noreply, State#state{is_paused = true}};

handle_cast(resume, State) ->
    {noreply, State#state{is_paused = false}};

handle_cast({disconnect, Target}, State) ->
    {noreply, disconnect_from_target(Target, State)};

%% reset all backoff delays to zero.
%% TODO: restart stalled connections.
handle_cast(reset_backoff, State) ->
    NewEps = reset_backoff(State#state.endpoints),
    {noreply, State#state{endpoints = NewEps}};

%% helper process says no endpoints were returned by the locators.
%% helper process will schedule a retry.
handle_cast({conmgr_no_endpoints, _Ref}, State) ->
    %% mark connection as black-listed and start timer for reset
    {noreply, State};

%% helper process says it failed to reach an address.
handle_cast({endpoint_failed, Addr, Reason, ProtocolId}, State) ->
    ?TRACE(?debugFmt("Failing endpoint ~p for protocol ~p with reason ~p", [Addr, ProtocolId, Reason])),
    %% mark connection as black-listed and start timer for reset
    {noreply, fail_endpoint(Addr, Reason, ProtocolId, State)}.

%% it is time to remove Addr from the black-listed addresses
handle_info({backoff_timer, Addr}, State = #state{endpoints = EPs}) ->
    case orddict:find(Addr, EPs) of
        {ok, EP} ->
            EP2 = EP#ep{is_black_listed = false},
            {noreply, State#state{endpoints = orddict:store(Addr,EP2,EPs)}};
        error ->
            %% TODO: Should never happen because the Addr came from the EP list.
            {norepy, State}
    end;
handle_info({retry_req, Ref}, State = #state{pending = Pending}) ->
    case lists:keyfind(Ref, #req.ref, Pending) of
        false ->
            %% TODO: should never happen
            {noreply, State};
        Req ->
            {noreply, start_request(Req, State)}
    end;
    
%%% All of the connection helpers end here
%% cases:
%% helper succeeded -> update EP stats: BL<-false, backoff_delay<-0
%% helper failed -> updates EP stats: failures++, backoff_delay++
%% other Pid failed -> pass on linked error
handle_info({'EXIT', From, Reason}, State = #state{pending = Pending}) ->
    %% Work out which endpoint it was
    case lists:keytake(From, #req.pid, Pending) of
        false ->
            %% Must have been something we were linked to, or linked to us
            ?TRACE(?debugFmt("Connection Manager exiting because linked process ~p exited for reason: ~p",
                        [From, Reason])),
            lager:error("Connection Manager exiting because linked process ~p exited for reason: ~p",
                        [From, Reason]),
            exit({linked, From, Reason});
        {value, #req{cur = Cur, ref = Ref}=Req, Pending2} ->
            {{ProtocolId, _Foo},_Bar} = Req#req.spec,
            case Reason of
                ok ->
                    %% update the stats module
                    Stat = conn_success,
                    ?TRACE(?debugMsg("Trying for stats update, the connect_endpoint")),
                    riak_core_connection_mgr_stats:update(Stat, Cur, ProtocolId),
                    %% riak_core_connection set up and handlers called
                    {noreply, connect_endpoint(Cur, State#state{pending = Pending2})};

                {ok, cancelled} ->
                    %% helper process has been cancelled and has exited nicely.
                    %% update the stats module
                    Stat = conn_cancelled,
                    ?TRACE(?debugMsg("Trying for stats update")),
                    riak_core_connection_mgr_stats:update(Stat, Cur, ProtocolId),
                    %% toss out the cancelled request from pending.
                    {noreply, State#state{pending = Pending2}};
                
                {error, endpoints_exhausted, Ref} ->
                    %% tried all known endpoints. schedule a retry.
                    %% reuse the existing request Reference, Ref.
                    case Req#req.state of
                        cancelled ->
                            %% oops. that request was cancelled. No retry
                            {noreply, State#state{pending = Pending2}};
                        _ ->
                            ?TRACE(?debugMsg("Scheduling retry")),
                            {noreply, schedule_retry(?EXHAUSTED_ENDPOINTS_RETRY_INTERVAL, Ref, State)}
                    end;

                Reason -> % something bad happened to the connection, reuse the request
                    ?TRACE(?debugFmt("handle_info: EP failed on ~p for ~p. removed Ref ~p",
                                     [Cur, Reason, Ref])),
                    lager:warning("handle_info: endpoint ~p failed: ~p. removed Ref ~p",
                                  [Cur, Reason, Ref]),
                    State2 = fail_endpoint(Cur, Reason, ProtocolId, State),
                    %% the connection helper will not retry. It's up the caller.
                    State3 = fail_request(Reason, Req, State2),
                    {noreply, State3}
            end
    end;
handle_info(_Unhandled, State) ->
    ?TRACE(?debugFmt("Unhandled gen_server info: ~p", [_Unhandled])),
    lager:error("Unhandled gen_server info: ~p", [_Unhandled]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Private
%%%===================================================================

identity_locator({IP,Port}, _Policy) ->
    {ok, [{IP,Port}]};
identity_locator([Ips], _Policy) ->
    {ok, Ips}.

%% close the pending connection and cancel the request
disconnect_from_target(Target, State = #state{pending = Pending}) ->
    lager:debug("Disconnecting from: ~p", [Target]),
    case lists:keyfind(Target, #req.target, Pending) of
        false ->
            %% already gone!
            State;
        Req ->
            %% The helper process will discover the cancellation when it asks if it
            %% should connect to an endpoint.
            State#state{pending = lists:keystore(Req#req.ref, #req.ref, Pending,
                                                 Req#req{state = cancelled})}
    end.

%% schedule a retry to occur after Interval milliseconds.
%% do not clear the pid from pending. the exit handler will do that.
schedule_retry(Interval, Ref, State = #state{pending = Pending}) ->
    case lists:keyfind(Ref, #req.ref, Pending) of
        false ->
            %% this should never happen
            lager:error("ConnectionManager: failed to find connection ref while scheduling retry."),
            State;
        Req ->
            case Req#req.state of
                cancelled ->
                    %% the request was cancelled, so no rescheduling wanted.
                    State;
                _ ->
                    %% reschedule request to happen in the future
                    erlang:send_after(Interval, self(), {retry_req, Ref}),
                    State#state{pending = lists:keystore(Req#req.ref, #req.ref, Pending,
                                                         Req#req{cur = undefined})}
            end
    end.

%% Start process to make connection to available endpoints. Return a reference for
%% this connection attempt.
start_request(#req{state=cancelled}, State) ->
    State;
start_request(Req = #req{ref=Ref, target=Target, spec=ClientSpec, strategy=Strategy},
              State) ->
    case locate_endpoints(Target, Strategy, State#state.locators) of
        {ok, []} ->
            %% locators provided no addresses
            gen_server:cast(?SERVER, {conmgr_no_endpoints, Ref}),
            Interval = app_helper:get_env(riak_core, connmgr_no_endpoint_retry,
                                         ?DEFAULT_RETRY_NO_ENDPOINTS),
            lager:debug("Connection Manager located no endpoints for: ~p. Will retry.", [Target]),
            %% schedule a retry and exit
            schedule_retry(Interval, Ref, State);
        {ok, EpAddrs } ->
            lager:debug("Connection Manager located endpoints: ~p", [EpAddrs]),
            AllEps = update_endpoints(EpAddrs, State#state.endpoints),
            TryAddrs = filter_blacklisted_endpoints(EpAddrs, AllEps),
            lager:debug("Connection Manager trying endpoints: ~p", [TryAddrs]),
            Pid = spawn_link(
                    fun() -> exit(try connection_helper(Ref, ClientSpec, Strategy, TryAddrs)
                                  catch T:R -> {exception, {T, R}}
                                  end)
                    end),
            State#state{endpoints = AllEps,
                        pending = lists:keystore(Ref, #req.ref, State#state.pending,
                                                 Req#req{pid = Pid,
                                                         state = connecting,
                                                         cur = undefined})};
        {error, Reason} ->
            fail_request(Reason, Req, State)
    end.

%% reset the backoff delay to zero for all endpoints
reset_backoff(Endpoints) ->
    orddict:map(fun(_Addr,EP) -> EP#ep{backoff_delay = 0} end,Endpoints).

%% increase the backoff delay, but cap at a maximum
increase_backoff(0) ->
    ?INITIAL_BACKOFF;
increase_backoff(Delay) when Delay > ?MAX_BACKOFF ->
    ?MAX_BACKOFF;
increase_backoff(Delay) ->
    2 * Delay.

%% Convert an inet:address to a string if needed.
string_of_ip(IP) when is_tuple(IP) ->    
    inet_parse:ntoa(IP);
string_of_ip(IP) ->
    IP.

string_of_ipport({IP,Port}) ->
    string_of_ip(IP) ++ ":" ++ erlang:integer_to_list(Port).

%% A spawned process that will walk down the list of endpoints and try them
%% all until exhausting the list. This process is responsible for waiting for
%% the backoff delay for each endpoint.
connection_helper(Ref, _Protocol, _Strategy, []) ->
    %% exhausted the list of endpoints. let server start new helper process
    {error, endpoints_exhausted, Ref};
connection_helper(Ref, Protocol, Strategy, [Addr|Addrs]) ->
    {{ProtocolId, _Foo},_Bar} = Protocol,
    %% delay by the backoff_delay for this endpoint.
    {ok, BackoffDelay} = gen_server:call(?SERVER, {get_endpoint_backoff, Addr}),
    lager:debug("Holding off ~p seconds before trying ~p at ~p",
               [(BackoffDelay/1000), ProtocolId, string_of_ipport(Addr)]),
    timer:sleep(BackoffDelay),
    case gen_server:call(?SERVER, {should_try_endpoint, Ref, Addr}) of
        true ->
            lager:debug("Trying connection to: ~p at ~p", [ProtocolId, string_of_ipport(Addr)]),
            ?TRACE(?debugMsg("Attempting riak_core_connection:sync_connect/2")),
            case riak_core_connection:sync_connect(Addr, Protocol) of
                ok ->
                    ok;
                {error, Reason} ->
                    %% notify connection manager this EP failed and try next one
                    gen_server:cast(?SERVER, {endpoint_failed, Addr, Reason, ProtocolId}),
                    connection_helper(Ref, Protocol, Strategy, Addrs)
            end;
        _ ->
            %% connection request has been cancelled
            lager:debug("Ignoring connection to: ~p at ~p because it was cancelled",
                       [ProtocolId, string_of_ipport(Addr)]),
            {ok, cancelled}
    end.

locate_endpoints({Type, Name}, Strategy, Locators) ->
    case orddict:find(Type, Locators) of
        {ok, Locate} ->
            case Locate(Name, Strategy) of
                error ->
                    {error, {bad_target_name_args, Type, Name}};
                Addrs ->
                    Addrs
            end;
        error ->
            {error, {unknown_target_type, Type}}
    end.

%% Make note of the failed connection attempt and update
%% our book keeping for that endpoint. Black-list it, and
%% adjust a backoff timer so that we wait a while before
%% trying this endpoint again.
fail_endpoint(Addr, Reason, ProtocolId, State) ->
    %% update the stats module
    Stat = {conn_error, Reason},
    riak_core_connection_mgr_stats:update(Stat, Addr, ProtocolId),
    %% update the endpoint
    Fun = fun(EP=#ep{backoff_delay = Backoff, failures = Failures}) ->
                  erlang:send_after(Backoff, self(), {backoff_timer, Addr}),
                  EP#ep{failures = orddict:update_counter(Reason, 1, Failures),
                        nb_failures = EP#ep.nb_failures + 1,
                        backoff_delay = increase_backoff(Backoff),
                        last_fail_time = os:timestamp(),
                        next_try_secs = Backoff/1000,
                        is_black_listed = true}
          end,
    update_endpoint(Addr, Fun, State).

connect_endpoint(Addr, State) ->
    update_endpoint(Addr, fun(EP) ->
                                  EP#ep{is_black_listed = false,
                                        nb_success = EP#ep.nb_success + 1,
                                        next_try_secs = 0,
                                        backoff_delay = 0}
                          end, State).

%% Return the current backoff delay for the named Address,
%% or if we can't find that address in the endpoints - the
%% initial backoff.
get_endpoint_backoff(Addr, EPs) ->
    case orddict:find(Addr, EPs) of
        error ->
            0;
        {ok, EP} ->
            EP#ep.backoff_delay
    end.

update_endpoint(Addr, Fun, State = #state{endpoints = EPs}) ->
    case orddict:find(Addr, EPs) of
        error ->
            EP2 = Fun(#ep{addr = Addr}),
            State#state{endpoints = orddict:store(Addr,EP2,EPs)};
        {ok, EP} ->
            EP2 = Fun(EP),
            State#state{endpoints = orddict:store(Addr,EP2,EPs)}
    end.

fail_request(Reason, #req{ref = Ref, spec = Spec},
             State = #state{pending = Pending}) ->
    %% Tell the module it failed
    {Proto, {_TcpOptions, Module,Args}} = Spec,
    ?TRACE(?debugFmt("module ~p getting connect_failed", [Module])),
    Module:connect_failed(Proto, {error, Reason}, Args),
    %% Remove the request from the pending list
    State#state{pending = lists:keydelete(Ref, #req.ref, Pending)}.

update_endpoints(Addrs, Endpoints) ->
    %% add addr to Endpoints if not already there
    Fun = (fun(Addr, EPs) ->
                   case orddict:is_key(Addr, Endpoints) of
                       true -> EPs;
                       false ->
                           EP = #ep{addr=Addr},
                           orddict:store(Addr, EP, EPs)
                   end
           end),
    lists:foldl(Fun, Endpoints, Addrs).

%% Return the addresses of non-blacklisted endpoints that are also
%% members of the list EpAddrs.
filter_blacklisted_endpoints(EpAddrs, AllEps) ->
    PredicateFun = (fun(Addr) ->
                            case orddict:find(Addr, AllEps) of
                                {ok, EP} ->
                                    EP#ep.is_black_listed == false;
                                error ->
                                    false
                            end
                    end),
    lists:filter(PredicateFun, EpAddrs).
