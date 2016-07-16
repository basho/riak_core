%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies, Inc.
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

-module(riak_core_job_manager).
-behaviour(gen_server).

% Public API
-export([
    group/1, group/2,
    job_svc/1,
    lookup/1,
    start_scope/1, start_scope/2,
    stop_scope/1, stop_scope/2,
    stop_scope_async/1
]).

% Public types
-export_type([
    scope_id/0,
    scope_index/0,
    scope_type/0
]).

% Private API
-export([
    register/2,
    start_link/1,
    submit_mult/2
]).

% gen_server callbacks
-export([
    code_change/3,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    init/1,
    terminate/2
]).

-include("riak_core_job_internal.hrl").

-define(SERVICE_NAME,   ?MODULE).

%
% The process dictionary is opaque to the main module code to allow for
% finding a reasonably performant implementation strategy. It could get
% pretty big, so it might matter.
%
% It needs to be searchable in some reasonably efficient manner on the
% following keys/#prec{} fields:
%
%   Proc/Scope Type:    ptype + stype
%   Process Label:      ptype + stype + sindex
%   Scope Id:           stype + sindex
%   Monitor Reference:  mon
%
% The ('ptype' + 'stype' + 'sindex'), 'pid', and 'mon' fields in #prec{} are
% unique across the process dictionary, so any of them appearing in more than
% one entry would be indicative of an error somewhere. Because of the cost of
% checking for such inconsistencies, however, don't assume they'll be caught.
%
% The uniqueness constraint is particularly relevant in the handling of
% Monitor References, as whenever one is removed through erasure or update
% of the #prec{} containing it, it is demonitored. Even if the reference is
% NOT found in the dictionary, any reference passed to pdict_erase() is
% demonitored.
%
% Note that pdict_demonitor/1 leaves the process dictionary in an invalid
% state and MUST only be used when the pdict is to be dropped or replaced in
% its entirety!
%
-spec pdict_new() -> pdict().
-spec pdict_demonitor(pdict()) -> 'ok'.
-spec pdict_erase(prec() | proc_id() | reference(), pdict()) -> pdict().
-spec pdict_find(proc_id() | reference(), pdict()) -> prec() | 'false'.
-spec pdict_group(scope_id(), pdict()) -> [prec()].
-spec pdict_group(proc_type(), scope_type(), pdict()) -> [prec()].
-spec pdict_store(prec(), pdict()) -> pdict().
-spec pdict_store(proc_id(), pid(), pdict()) -> pdict().
%
% proc_id() is {ptype, {stype, sindex}}
%
-record(prec,   {
    ptype       ::  proc_type(),
    stype       ::  scope_type(),
    sindex      ::  scope_index(),
    pid         ::  pid(),
    mon         ::  reference()
}).
-type prec()    :: #prec{}.

%
% Thankfully, the configuration dictionary is a simple mapping from
%   scope_type() => riak_core_job_service:config()
% It's presumably pretty small and stable, so it's just a list for now.
% In OTP-18+ it may become a map ... or not.
%
-type cdict()   ::  [{scope_type(), riak_core_job_service:config()}].

-record(state, {
    jobs_sup                ::  pid(),
    svc_name                ::  atom(),
    cdict   = []            ::  cdict(),
    pdict   = pdict_new()   ::  pdict()
}).
-type state()   ::  #state{}.

%% ===================================================================
%% Public API
%% ===================================================================

-spec start_scope(scope_id()) -> 'ok' | {'error', term()}.
%%
%% @doc Add a per-scope tree to the top-level supervisor.
%%
%% If the scope is not already running and a scope of the same type has
%% previously been started with a configuration specification, the new scope
%% is started using that configuration.
%%
%% If multiple nodes of the same type have been started with different
%% configurations, it's unspecified which one is used to start the new scope,
%% but you wouldn't do that - right?
%%
%% Possible return values are:
%%
%% 'ok' - The scope process tree is running.
%%
%% `{error, Reason}' - An error occurred starting one of the processes.
%%
%% `{error, noproc}' - The service is not available, probably meaning
%% riak_core is hosed and this is the least of your problems.
%%
start_scope({SType, _} = ScopeID) when erlang:is_atom(SType) ->
    gen_server:call(?SERVICE_NAME,
        {'start_scope', ScopeID}, ?SCOPE_SVC_STARTUP_TIMEOUT).

-spec start_scope(scope_id(), riak_core_job_service:config())
        -> 'ok' | {'error', term()}.
%%
%% @doc Add a per-scope tree to the top-level supervisor.
%%
%% Possible return values are:
%%
%% 'ok' - The scope process tree is running. If it was already running, it may
%% be configured differently than specified in Config.
%%
%% `{error, Reason}' - An error occurred starting one of the processes.
%%
%% `{error, noproc}' - The service is not available, probably meaning
%% riak_core is hosed and this is the least of your problems.
%%
start_scope({SType, _} = ScopeID, Config)
        when erlang:is_atom(SType) andalso erlang:is_list(Config) ->
    gen_server:call(?SERVICE_NAME,
        {'start_scope', ScopeID, Config}, ?SCOPE_SVC_STARTUP_TIMEOUT).

-spec stop_scope_async(scope_id()) -> 'ok'.
%%
%% @doc Shut down the per-scope tree asynchronously.
%%
%% Signals the per-scope tree to shut down and returns immediately.
%% To wait for the shutdown to complete, use stop_scope/1 or stop_scope/2.
%%
stop_scope_async({SType, _} = ScopeID) when erlang:is_atom(SType) ->
    gen_server:cast(?SERVICE_NAME, {'stop_scope', ScopeID}).

-spec stop_scope(scope_id()) -> 'ok' | {'error', term()}.
%%
%% @doc Shut down the per-scope tree synchronously.
%%
%% This just calls stop_scope/2 with a default timeout.
%%
stop_scope(ScopeID) ->
    stop_scope(ScopeID, ?STOP_SCOPE_TIMEOUT).

-spec stop_scope(scope_id(), non_neg_integer() | 'infinity')
        -> 'ok' | {'error', term()}.
%%
%% @doc Shut down the per-scope tree semi-synchronously.
%%
%% Immediately signals the per-scope tree to shut down and waits up to Timeout
%% milliseconds for it to complete. In all cases (unless the service itself has
%% crashed) the shutdown runs to completion - the result only indicates whether
%% it completes within the specified timeout.
%%
%% If Timeout is greater than 32 bits, it is treated as 'infinity' due to
%% (quite reasonable) limitations in Erlang millisecond timers.
%% Refer to the 'receive' expression's 'after' clause or assorted timeout docs.
%%
%% Possible return values are:
%%
%% 'ok' - The tree was not running, or shutdown completed within Timeout ms.
%%
%% `{error, timeout}' - The shutdown continues in the background.
%%
%% `{error, noproc}' - The service is not available, probably meaning
%% riak_core is hosed and this is the least of your problems.
%%
%% If Timeout is zero, the function returns immediately, with 'ok' indicating
%% the tree wasn't running, or {error, timeout} indicating the shutdown was
%% initiated as if by stop_scope_async/1.
%%
%% If Timeout is 'infinity' (or greater than 32 bits) the function waits
%% indefinitely for the tree to shut down. However, internal shutdown timeouts
%% in the supervisors should cause the tree to shut down in well under a minute
%% unless the system is badly screwed up.
%%
%% @see http://erlang.org/doc/reference_manual/expressions.html
%%
stop_scope(ScopeID, Timeout)
        when erlang:is_integer(Timeout) andalso Timeout >= (1 bsl 32) ->
    stop_scope(ScopeID, 'infinity');
stop_scope({SType, _} = ScopeID, Timeout)
        when erlang:is_atom(SType) andalso (Timeout =:= 'infinity'
        orelse (erlang:is_integer(Timeout) andalso Timeout >= 0)) ->
    % The gen_server call returns the pid of the scope's top-level supervisor
    % after spawning a process to shut down the tree. We monitor the pid here
    % rather than in the gen_server for two reasons:
    % 1)  Avoid a late response landing in the caller's mailbox (assuming the
    %     timeout exception didn't kill the caller outright), where it would be
    %     spurious at best and possibly cause problems if it's not recognized.
    % 2)  Avoid having to handle the state and messages in the server to
    %     recognize when the scope has shut down and reply back to here. This
    %     is actually a much bigger deal than the above.
    case gen_server:call(?SERVICE_NAME, {'stop_scope', ScopeID}) of
        {'ok', Sup} ->
            Ref = erlang:monitor('process', Sup),
            receive
                {'DOWN', Ref, _, Sup, _} ->
                    'ok'
            after
                Timeout ->
                    _ = erlang:demonitor(Ref, ['flush']),
                    {'error', 'timeout'}
            end;
        Reply ->
            Reply
    end.

-spec job_svc(scope_svc_id() | scope_id()) -> pid() | {'error', term()}.
%%
%% @doc So much like start_scope/1 that it may not be worth keeping it.
%%
job_svc(?SCOPE_SVC_ID({SType, _}) = ProcID) when erlang:is_atom(SType) ->
    case gen_server:call(?SERVICE_NAME,
            {'job_svc', ProcID}, ?SCOPE_SVC_STARTUP_TIMEOUT) of
        'lookup' ->
            gen_server:call(?SERVICE_NAME, {'lookup', ProcID});
        Ret ->
            Ret
    end;
job_svc({SType, _} = ScopeID) when erlang:is_atom(SType) ->
    job_svc(?SCOPE_SVC_ID(ScopeID)).

-spec lookup(proc_id()) -> pid() | 'undefined' | {'error', term()}.
%%
%% @doc Find the pid of the specified process.
%%
%% Unlike job_svc/1, this NEVER starts the process.
%%
lookup({PType, {SType, _}} = ProcID)
        when erlang:is_atom(PType) andalso erlang:is_atom(SType) ->
    gen_server:call(?SERVICE_NAME, {'lookup', ProcID}).

-spec group(scope_id()) -> [{proc_id(), pid()}].
%%
%% @doc Find the running (non-runner) processes for a scope.
%%
group({SType, _} = ScopeID) when erlang:is_atom(SType) ->
    gen_server:call(?SERVICE_NAME, {'group', ScopeID}).

-spec group(proc_type(), scope_type()) -> [{proc_id(), pid()}].
%%
%% @doc Find the running processes of a specified type for a scope type.
%%
group(PType, SType) when erlang:is_atom(PType) andalso erlang:is_atom(SType) ->
    gen_server:call(?SERVICE_NAME, {'group', PType, SType}).

-spec register(proc_id(), pid()) -> 'ok'.
%%
%% @doc Register the specified process.
%%
register({PType, {SType, _}} = ProcID, Pid) when erlang:is_pid(Pid)
        andalso erlang:is_atom(PType) andalso erlang:is_atom(SType) ->
    gen_server:cast(?SERVICE_NAME, {'register', ProcID, Pid}).

%% ===================================================================
%% Private API
%% ===================================================================

-spec start_link(pid()) -> {'ok', pid()} | {'error', term()}.
start_link(JobsSup) ->
    SvcName = ?SERVICE_NAME,
    gen_server:start_link({'local', SvcName}, ?MODULE, {JobsSup, SvcName}, []).

-spec submit_mult(scope_type() | [scope_id()], riak_core_job:job())
        -> 'ok' | {'error', term()}.
%
% This function is for use by riak_core_job_service:submit/2 ONLY!
%
% The operation has to be coordinated from a process other than the jobs
% service because the service, and managers, need to continue handling the
% messages that allow the prepare/commit sequence.
%
% All messages include a reference() used only for this operation, and the
% pid() of the coordinating process (whoever called this).
%
submit_mult(Where, Job) when erlang:is_atom(Where)
        orelse (erlang:is_list(Where) andalso erlang:length(Where) > 0) ->
    case riak_core_job:version(Job) of
        {'job', _} ->
            multi_client(Where, Job);
        _ ->
            erlang:error('badarg', [Job])
    end;
submit_mult(Selector, _) ->
    erlang:error('badarg', [Selector]).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

-spec init({pid(), atom()}) -> {'ok', state()}.
init({JobsSup, SvcName}) ->
    erlang:process_flag('trap_exit', 'true'),
    % At startup, crawl the supervision tree to populate the state. The only
    % time this will find anything is if this service crashed and is being
    % restarted by the supervisor, which shouldn't be happening, but we do
    % want to recover if it does.
    % We can't call which_children/1 on the supervisor here, because we're
    % (presumably) already in a synchronous operation in the supervisor so the
    % call wouldn't be handled until after we return. Instead, plant a special
    % message in our own inbox to complete the initialization there.
    % Since our own server isn't initialized yet, use an async cast, as a
    % synchronous call would wedge for the same reason the one to the
    % supervisor would.
    ?cast('init'),
    {'ok', #state{jobs_sup = JobsSup, svc_name = SvcName}}.

-spec handle_call(term(), {pid(), term()}, state())
        -> {'reply', term(), state()}.
%
% lookup(proc_id()) -> pid() | 'undefined'
%
handle_call({'lookup', ProcID}, _, #state{pdict = Procs} = State) ->
    case pdict_find(ProcID, Procs) of
        #prec{pid = Pid} ->
            {'reply', Pid, State};
        'false' ->
            {'reply', 'undefined', State}
    end;
%
% group(scope_id()) -> [{proc_id(), pid()}]
%
handle_call({'group', ScopeID}, _, #state{pdict = Procs} = State) ->
    Ret = [{{PType, ScopeID}, Pid}
            || #prec{ptype = PType, pid = Pid}
                <- pdict_group(ScopeID, Procs)],
    {'reply', Ret, State};
%
% group(proc_type(), scope_type()) -> [{proc_id(), pid()}]
%
handle_call({'group', PType, SType}, _, #state{pdict = Procs} = State) ->
    Ret = [{{PType, {SType, SIndex}}, Pid}
            || #prec{sindex = SIndex, pid = Pid}
                <- pdict_group(PType, SType, Procs)],
    {'reply', Ret, State};
%
% job_svc(node_mgr_id()) -> pid()
%
handle_call({'job_svc', ?SCOPE_SVC_ID({SType, _} = ScopeID) = ProcID}, _,
        #state{jobs_sup = Sup, pdict = Procs, cdict = Cfgs} = State) ->
    case pdict_find(ProcID, Procs) of
        #prec{pid = Pid} ->
            {'reply', Pid, State};
        _ ->
            Config = case lists:keyfind(SType, 1, Cfgs) of
                'false' ->
                    [];
                {_, Cfg} ->
                    Cfg
            end,
            case riak_core_job_sup:start_scope(Sup, ScopeID, Config) of
                {'ok', _} ->
                    % There's a 'register' message with the service's pid in
                    % our inbox right now, but we can't get to it cleanly from
                    % here, so tell the calling process to look it up.
                    {'reply', 'lookup', State};
                {'error', _} = Err ->
                    {'reply', Err, State}
            end
    end;
%
% submit_mult(scope_type() | [scope_id()], job()) -> ok | {error, term()}
%
handle_call(Msg, {Client, _}, State)
        when erlang:is_tuple(Msg)
        andalso erlang:tuple_size(Msg) > 0
        andalso erlang:element(1, Msg) =:= 'submit_mult' ->
    multi_server(Msg, Client, State);
%
% start_scope(scope_id()) -> 'ok' | {'error', term()}
%
handle_call({'start_scope', {SType, _} = ScopeID}, _,
        #state{jobs_sup = Sup, pdict = Procs, cdict = Cfgs} = State) ->
    case pdict_find(?SCOPE_SVC_ID(ScopeID), Procs) of
        #prec{} ->
            {'reply', 'ok', State};
        _ ->
            Config = case lists:keyfind(SType, 1, Cfgs) of
                'false' ->
                    [];
                {_, Cfg} ->
                    Cfg
            end,
            case riak_core_job_sup:start_scope(Sup, ScopeID, Config) of
                {'ok', _} ->
                    {'reply', 'ok', State};
                {'error', _} = Err ->
                    {'reply', Err, State}
            end
    end;
%
% start_scope(scope_id(), config()) -> 'ok' | {'error', term()}
%
handle_call({'start_scope', {SType, _} = ScopeID, Config}, _,
        #state{jobs_sup = Sup, pdict = Procs, cdict = Cfgs} = State) ->
    case pdict_find(?SCOPE_SVC_ID(ScopeID), Procs) of
        #prec{} ->
            {'reply', 'ok', State};
        _ ->
            case riak_core_job_sup:start_scope(Sup, ScopeID, Config) of
                {ok, _} ->
                    case lists:keyfind(SType, 1, Cfgs) of
                        'false' ->
                            {'reply', 'ok',
                                State#state{cdict = [{SType, Config} | Cfgs]}};
                        % TODO: Is this the best way to handle this?
                        % We want the latest successful config in the cache,
                        % but comparing them is expensive. OTOH, just blindly
                        % doing a keystore means a list copy and state update,
                        % which could easily be even more costly.
                        % Assuming the config was created by the same code per
                        % scope type, the order of the elements is likely the
                        % same, so just compare the object as a whole.
                        {_, Config} ->
                            {'reply', 'ok', State};
                        _ ->
                            {'reply', 'ok', State#state{cdict =
                                lists:keystore(SType, 1, Cfgs, {SType, Config})}}
                    end;
                {'error', _} = Err ->
                    {'reply', Err, State}
            end
    end;
%
% stop_scope(scope_id(), Timeout) -> 'ok' | {'error', term()}
%
handle_call({'stop_scope', ScopeID}, _, State) ->
    case begin_stop_scope(ScopeID, State) of
        {{'error', 'noproc'}, NewState} ->
            {'reply', 'ok', NewState};
        {Reply, NewState} ->
            {'reply', Reply, NewState}
    end;
%
% unrecognized message
%
handle_call(Msg, From, #state{svc_name = Name} = State) ->
    _ = lager:error("~p service received unhandled call from ~p: ~p",
            [Name, From, Msg]),
    {'reply', {'error', {'badarg', Msg}}, State}.

-spec handle_cast(term(), state()) -> {'noreply', state()}.
%
% submit_mult(scope_type() | [scope_id()], job()) -> ok | {error, term()}
%
handle_cast(Msg, State)
        when erlang:is_tuple(Msg)
        andalso erlang:tuple_size(Msg) > 0
        andalso erlang:element(1, Msg) =:= 'submit_mult' ->
    multi_server(Msg, State);
%
% register(work_sup_id(), pid()) -> 'ok'
% the per-scope work supervisor gets special handling
%
handle_cast({'register', ?WORK_SUP_ID(ScopeID) = ProcID, Sup}, StateIn) ->
    State = StateIn#state{pdict = pdict_store(ProcID, Sup, StateIn#state.pdict)},
    case pdict_find(?SCOPE_SVC_ID(ScopeID), State#state.pdict) of
        #prec{pid = Svc} ->
            _ = riak_core_job_service:register(Svc, ProcID, Sup),
            {'noreply', State};
        _ ->
            % in case things are getting scheduled weird, retry a few times
            _ = ?cast({'retry_work_reg', 5, ProcID, Sup}),
            {'noreply', State}
    end;
%
% register(proc_id(), pid()) -> 'ok'
%
handle_cast({'register', ProcID, Pid}, #state{pdict = Procs} = State) ->
    {'noreply', State#state{pdict = pdict_store(ProcID, Pid, Procs)}};
%
% special message to retry registering a work supervisor with its service
% confirm the supervisor's pid to make sure it's still registered itself
%
handle_cast({'retry_work_reg', Count, ?WORK_SUP_ID(ScopeID) = ProcID, Sup},
        #state{svc_name = Name, pdict = Procs} = State) ->
    case pdict_find(ProcID, Procs) of
        #prec{pid = Sup} ->
            case pdict_find(?SCOPE_SVC_ID(ScopeID), Procs) of
                #prec{pid = Svc} ->
                    _ = riak_core_job_service:register(Svc, ProcID, Sup),
                    {'noreply', State};
                _ ->
                    Next = (Count - 1),
                    if
                        Next > 0 ->
                            _ = ?cast({'retry_work_reg', Next, ProcID, Sup}),
                            {'noreply', State};
                        ?else ->
                            _ = lager:error("~p service stranded ~p: ~p",
                                    [Name, ProcID, Sup]),
                            {'noreply', State}
                    end
            end;
        _ ->
            {'noreply', State}
    end;
%
% stop_scope_async(scope_id()) -> 'ok'
%
handle_cast({'stop_scope', ScopeID}, State) ->
    {_, NewState} = begin_stop_scope(ScopeID, State),
    NewState;
%
% placed here once by init/2 at startup - see the comment there about why
% we'll never see this again, so it's the last pattern to handle
%
handle_cast('init', #state{
        jobs_sup = Sup, pdict = Procs, cdict = Cfgs} = State) ->
    {CD, PD} = absorb_sup_tree(supervisor:which_children(Sup), {Cfgs, Procs}),
    {'noreply', State#state{cdict = CD, pdict = PD}};
%
% unrecognized message
%
handle_cast(Msg, #state{svc_name = Name} = State) ->
    _ = lager:error("~p service received unhandled cast: ~p", [Name, Msg]),
    {'noreply', State}.

-spec handle_info(term(), state()) -> {'noreply', state()}.
%
% submit_mult(scope_type() | [scope_id()], job()) -> ok | {error, term()}
%
handle_info(Msg, State)
        when erlang:is_tuple(Msg)
        andalso erlang:tuple_size(Msg) > 0
        andalso erlang:element(1, Msg) =:= 'submit_mult' ->
    multi_server(Msg, State);
%
% a monitored process exited
%
handle_info({'DOWN', Mon, _, _, _}, #state{pdict = Procs} = State) ->
    {'noreply', State#state{pdict = pdict_erase(Mon, Procs)}};
%
% unrecognized message
%
handle_info(Msg, #state{svc_name = Name} = State) ->
    _ = lager:error("~p service received unhandled info: ~p", [Name, Msg]),
    {'noreply', State}.

-spec terminate(term(), state()) -> ok.
%
% no matter why we're terminating, de-monitor everything
%
terminate(_, #state{pdict = Procs}) ->
    pdict_demonitor(Procs).

-spec code_change(term(), state(), term()) -> {ok, state()}.
%
% at present we don't care, so just carry on
%
code_change(_, State, _) ->
    {'ok', State}.

%% ===================================================================
%% Internal
%% ===================================================================

-spec absorb_sup_tree(
    [{term(), pid() | 'undefined', 'worker' | 'supervisor', [module()]}],
    {cdict(), pdict()})
        -> {cdict(), pdict()}.
%
% Called indirectly by init/1 to repopulate state on restart.
%
absorb_sup_tree([{'riak_core_job_manager', _, _, _} | Rest], Dicts) ->
    absorb_sup_tree(Rest, Dicts);

absorb_sup_tree([{_, Ch, _, _} | Rest], Dicts) when not erlang:is_pid(Ch) ->
    absorb_sup_tree(Rest, Dicts);

absorb_sup_tree(
        [{?SCOPE_SVC_ID({SType, _}) = ProcID, Pid, _, _} | Rest], {CDIn, PD}) ->
    % See if we can grab a config we don't already have. There's no intelligent
    % way to get the latest one in the current situation, so take the first of
    % each type.
    CD = case lists:keyfind(SType, 1, CDIn) of
        'false' ->
            case riak_core_job_service:config(Pid) of
                [_|_] = Config ->
                    [{SType, Config} | CDIn];
                _ ->
                    CDIn
            end;
        _ ->
            CDIn
    end,
    absorb_sup_tree(Rest, {CD, pdict_store(ProcID, Pid, PD)});

absorb_sup_tree([{?WORK_SUP_ID(_) = ProcID, Pid, _, _} | Rest], {CD, PD}) ->
    % Don't descend into work runner supervisors. It would be preferable to
    % be able to check the supervisor's restart strategy, but we can't get to
    % that through the public API.
    absorb_sup_tree(Rest, {CD, pdict_store(ProcID, Pid, PD)});

absorb_sup_tree([{ProcID, Pid, 'supervisor', _} | Rest], {CD, PD}) ->
    absorb_sup_tree(Rest, absorb_sup_tree(
        supervisor:which_children(Pid), {CD, pdict_store(ProcID, Pid, PD)}));

absorb_sup_tree([{ProcID, Pid, _, _} | Rest], {CD, PD}) ->
    absorb_sup_tree(Rest, {CD, pdict_store(ProcID, Pid, PD)});

absorb_sup_tree([], Dicts) ->
    Dicts.

-spec begin_stop_scope(scope_id(), state())
        -> {{'ok', pid()} | {'error', term()}, state()}.
%
% Spawns an unlinked process to stop the specified scope asynchronously.
%
begin_stop_scope(ScopeID, #state{jobs_sup = Sup, pdict = Procs} = State) ->
    case pdict_find(?SCOPE_SUP_ID(ScopeID), Procs) of
        #prec{pid = Pid} ->
            _ = erlang:spawn('riak_core_job_sup', 'stop_scope', [Sup, ScopeID]),
            {{'ok', Pid}, State};
        'false' ->
            {{'error', 'noproc'}, State}
    end.

%
% multi_client/2 and multi_server/N are tightly coupled
% multi_client/2 executes (waits) in the originating process
% multi_server/N executes in the servicing gen_server process
%
% The originator will already have a monitor on the service, and will pass it
% into multi_client/3 to include it in its receive block. The Ref is included
% in all messages relating to this job submission.
%
multi_client(Where, Job) ->
    case erlang:whereis(?SERVICE_NAME) of
        'undefined' ->
            {'error', 'noproc'};
        Svc ->
            Mon = erlang:monitor('process', Svc),
            Ref = erlang:make_ref(),
            case gen_server:call(Svc, {'submit_mult', Ref, Where, Job}) of
                Ref ->
                    receive
                        {'DOWN', Mon, _, Svc, Info} ->
                            {'error', {'noproc', Info}};
                        {Ref, Result} ->
                            _ = erlang:demonitor(Mon, ['flush']),
                            Result
                    end;
                Other ->
                    _ = erlang:demonitor(Mon, ['flush']),
                    Other
            end
    end.
%
% receives any message coming into handle_call/3 that is a tuple whose first
% element is 'submit_mult' and returns {reply, Response, State}
%
multi_server({Tag, Ref, _Where, _Job}, Client, State) ->
    %
    % We can dispatch the prepare messages to the managers from here, but then
    % we need to return so this process can handle the messages coming back,
    % signal the commit or rollback, clean up, and provide the result.
    %
    % Even though we're just returning a simple error, the client is waiting
    % for the reference it sent in, so we put a message in our own inbox to
    % reply asynchronously with the error after we've successfully returned
    % from the synchronous call.
    %
    Result = {'error', 'not_implemented'},
    _ = ?cast({Tag, Ref, Client, 'result', Result}),
    %
    % give the client back what it expects
    %
    {'reply', Ref, State}.
%
% receives any message coming into handle_cast/2 or handle_info/2 that is a
% tuple whose first element is 'submit_mult' and returns {noreply, State}
%
multi_server({_Tag, Ref, Client, 'result', Result}, State) ->
    _ = erlang:send(Client, {Ref, Result}),
    {'noreply', State}.

%%
%% Process dictionary implementation strategies.
%% Each must define the 'pdict()' type and pdict_xxx() functions spec'd
%% at the top of the file.
%%
-define(pdict_list, 'true').
% -ifdef(namespaced_types).
% -define(pdict_map,  true).
% -else.
% -define(pdict_dict, true).
% -endif.

-ifdef(pdict_list).
%
% Simple and probably slow, but that's ok until we're sure what it needs to be
% able to do.
% There are SO many ways this could be optimized, but it's unlikely a list is
% the best way to manage this in the long run, so it's just a straightforward
% recursive implementation.
% The lists:* operations to mutate the list aren't used because they all
% preserve the order of the list and we don't care.
% TODO: Implement this with maps, or dicts, or ets?
%
-type pdict() :: [prec()].

-compile({inline, pdict_new/0}).
pdict_new() ->
    [].

pdict_demonitor([#prec{mon = Mon} | Procs]) ->
    _ = erlang:demonitor(Mon, ['flush']),
    pdict_demonitor(Procs);
pdict_demonitor([]) ->
    'ok'.

pdict_erase(#prec{mon = Mon}, Procs) ->
    pdict_erase(Mon, Procs);

pdict_erase({PType, {SType, SIndex}}, Procs) ->
    {Rec, NewProcs} = pdict_take_key(PType, SType, SIndex, Procs, []),
    case Rec of
        #prec{mon = Mon} ->
            _ = erlang:demonitor(Mon, ['flush']),
            NewProcs;
        'false' ->
            NewProcs
    end;

pdict_erase(Mon, Procs) ->
    _ = erlang:demonitor(Mon, ['flush']),
    {_, NewProcs} = pdict_take_mon(Mon, Procs, []),
    NewProcs.

pdict_find({PType, {SType, SIndex}}, Procs) ->
    pdict_find_key(PType, SType, SIndex, Procs);

pdict_find(Mon, Procs) ->
    pdict_find_mon(Mon, Procs).

-compile({inline, pdict_group/2}).
pdict_group({SType, SIndex}, Procs) ->
    pdict_find_scope(SType, SIndex, Procs, []).

-compile({inline, pdict_group/3}).
pdict_group(PType, SType, Procs) ->
    pdict_find_types(PType, SType, Procs, []).

-compile({nowarn_unused_function, {pdict_store, 2}}).
%
% Add or replace the specified record.
%
pdict_store(#prec{
        ptype = PType, stype = SType, sindex = SIndex} = Rec, Procs) ->
    case pdict_find_key(PType, SType, SIndex, Procs) of
        'false' ->
            [Rec | Procs];
        Rec ->
            Procs;
        _ ->
            pdict_replace(Rec, Procs, [])
    end.

%
% Add or replace the specified record.
%
pdict_store({PType, {SType, SIndex}}, Pid, Procs)
        when erlang:is_pid(Pid) ->
    case pdict_find_key(PType, SType, SIndex, Procs) of
        #prec{pid = Pid} ->
            Procs;
        Found ->
            Rec = #prec{
                ptype = PType, stype = SType, sindex = SIndex, pid = Pid,
                mon = erlang:monitor('process', Pid)},
            if
                Found =:= 'false' ->
                    [Rec | Procs];
                ?else ->
                    pdict_replace(Rec, Procs, [])
            end
    end.

%
% pdict internal
%
% pdict_replace_* and pdict_take_* always rewrite the list, so it's best to
% avoid calling them without being reasonably sure the record's already present
%
% for all of these functions, keys are always exploded before calling them
%

-spec pdict_find_key(
        proc_type(), scope_type(), scope_index(), [prec()])
        -> prec() | 'false'.
%
% Find the record with the specified key fields.
%
pdict_find_key(PType, SType, SIndex,
    [#prec{ptype = PType, stype = SType, sindex = SIndex} = Rec | _]) ->
    Rec;
pdict_find_key(PType, SType, SIndex, [_ | Procs]) ->
    pdict_find_key(PType, SType, SIndex, Procs);
pdict_find_key(_, _, _, []) ->
    'false'.

-spec pdict_find_key(
        proc_type(), scope_type(), scope_index(), [prec()], non_neg_integer())
        -> {non_neg_integer(), prec()} | 'false'.
-compile({nowarn_unused_function, {pdict_find_key, 5}}).
%
% Find the record with the specified key fields.
% Returns the record and the specified Count incremented by the number of
% records preceeding it in the list, such that calling with Count == 0 and
% passing the returned count to lists:split/2 returns the matching record as
% the head of the second result list.
%
pdict_find_key(PType, SType, SIndex,
    [#prec{ptype = PType, stype = SType, sindex = SIndex} = Rec | _], Count) ->
    {Count, Rec};
pdict_find_key(PType, SType, SIndex, [_ | Procs], Count) ->
    pdict_find_key(PType, SType, SIndex, Procs, (Count + 1));
pdict_find_key(_, _, _, [], _) ->
    'false'.

-spec pdict_find_mon(reference(), [prec()]) -> prec() | 'false'.
%
% Find the record with the specified monitor reference.
%
% pdict_find_mon(Mon, [#prec{mon = Mon} = Rec | _]) ->
%     Rec;
% pdict_find_mon(Mon, [_ | Procs]) ->
%     pdict_find_mon(Mon, Procs);
% pdict_find_mon(_, []) ->
%     'false'.
%
% lists:keyfind/2 is a bif, so probably faster even though it's generalized.
%
-compile({inline, pdict_find_mon/2}).
pdict_find_mon(Mon, Procs) ->
    lists:keyfind(Mon, #prec.mon, Procs).

-spec pdict_find_scope(scope_type(), scope_index(), [prec()], [prec()])
        -> [prec()].
%
% Return all records for the specified scope.
% Call with Result == [].
%
pdict_find_scope(SType, SIndex,
        [#prec{stype = SType, sindex = SIndex} = Rec | Procs], Result) ->
    pdict_find_scope(SType, SIndex, Procs, [Rec | Result]);
pdict_find_scope(SType, SIndex, [_ | Procs], Result) ->
    pdict_find_scope(SType, SIndex, Procs, Result);
pdict_find_scope(_, _, [], Result) ->
    Result.

-spec pdict_find_types(proc_type(), scope_type(), [prec()], [prec()])
        -> [prec()].
%
% Return all records of the specified process and scope types.
% Call with Result == [].
%
pdict_find_types(PType, SType,
        [#prec{ptype = PType, stype = SType} = Rec | Procs], Result) ->
    pdict_find_types(PType, SType, Procs, [Rec | Result]);
pdict_find_types(PType, SType, [_ | Procs], Result) ->
    pdict_find_types(PType, SType, Procs, Result);
pdict_find_types(_, _, [], Result) ->
    Result.

-spec pdict_take_key(
        proc_type(), scope_type(), scope_index(), [prec()], [prec()])
        -> {prec() | 'false', [prec()]}.
%
% Remove and return the record with the specified key.
% Call with Before == [].
%
pdict_take_key(PType, SType, SIndex,
        [#prec{ptype = PType, stype = SType, sindex = SIndex} = Rec
        | After], Before) ->
    {Rec, Before ++ After};
pdict_take_key(PType, SType, SIndex, [Rec | After], Before) ->
    pdict_take_key(PType, SType, SIndex, After, [Rec | Before]);
pdict_take_key(_, _, _, [], Before) ->
    {'false', Before}.

-spec pdict_take_mon(reference(), [prec()], [prec()])
        -> {prec() | 'false', [prec()]}.
%
% Remove and return the record with the specified key.
% Call with Before == [].
%
pdict_take_mon(Mon, [#prec{mon = Mon} = Rec | After], Before) ->
    {Rec, Before ++ After};
pdict_take_mon(Mon, [Rec | After], Before) ->
    pdict_take_mon(Mon, After, [Rec | Before]);
pdict_take_mon(_, [], Before) ->
    {'false', Before}.

-spec pdict_replace(prec(), [prec()], [prec()]) -> [prec()].
%
% Replaces the record whose ptype+stype+sindex equals that of the new record.
% Call with Before == [].
%
pdict_replace(#prec{
        ptype = PType, stype = SType, sindex = SIndex, mon = Mon} = NewRec,
        [#prec{ptype = PType, stype = SType, sindex = SIndex, mon = Mon}
        | After], Before) ->
    % this shouldn't occur, but we don't want to demonitor in the following
    % head if it's somehow called with an existing monitor
    Before ++ [NewRec | After];
pdict_replace(#prec{ptype = PType, stype = SType, sindex = SIndex} = NewRec,
        [#prec{ptype = PType, stype = SType, sindex = SIndex, mon = Mon}
        | After], Before) ->
    _ = erlang:demonitor(Mon, ['flush']),
    Before ++ [NewRec | After];
pdict_replace(NewRec, [Rec | After], Before) ->
    pdict_replace(NewRec, After, [Rec | Before]);
pdict_replace(NewRec, [], Before) ->
    [NewRec | Before].

-endif.

