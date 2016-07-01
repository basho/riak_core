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
%   Process Label:      ptype + stype + six
%   Node Id:            stype + six
%   Monitor Reference:  mon
%
% The ('ptype' + 'stype' + 'six'), 'pid', and 'mon' fields in #prec{} are
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
% proc_id() is {ptype, {stype, six}}
%
-record(prec,   {
    ptype       ::  proc_type(),
    stype       ::  scope_type(),
    six         ::  scope_index(),
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
    pdict   = pdict_new()   ::  pdict(),
    % passed to each job service on startup to test their accept callback
    dummy   = riak_core_job:dummy() ::  riak_core_job:job()
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
start_scope({Type, _} = ScopeID) when erlang:is_atom(Type) ->
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
start_scope({Type, _} = ScopeID, Config)
        when erlang:is_atom(Type) andalso erlang:is_list(Config) ->
    gen_server:call(?SERVICE_NAME,
        {'start_scope', ScopeID, Config}, ?SCOPE_SVC_STARTUP_TIMEOUT).

-spec stop_scope_async(scope_id()) -> 'ok'.
%%
%% @doc Shut down the per-scope tree asynchronously.
%%
%% Signals the per-scope tree to shut down and returns 'ok' immediately.
%% To wait for the shutdown to complete, use stop_scope/2.
%%
stop_scope_async({Type, _} = ScopeID) when erlang:is_atom(Type) ->
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
stop_scope(ScopeID, Timeout)
        when erlang:is_integer(Timeout) andalso Timeout >= (1 bsl 32) ->
    stop_scope(ScopeID, 'infinity');
stop_scope({Type, _} = ScopeID, Timeout) when erlang:is_atom(Type)
        andalso erlang:is_integer(Timeout) andalso Timeout >= 0 ->
    case lookup(?SCOPE_SUP_ID(ScopeID)) of
        Sup when erlang:is_pid(Sup) ->
            % Use monitor+cast instead of call+reply to avoid a late response
            % landing in the caller's mailbox, where it would be spurious at
            % best and possibly cause problems if it's not recognized.
            Ref = erlang:monitor('process', Sup),
            gen_server:cast(?SERVICE_NAME, {'stop_scope', ScopeID}),
            receive
                {'DOWN', Ref, _, Sup, _} ->
                    'ok'
            after
                Timeout ->
                    _ = erlang:demonitor(Ref, ['flush']),
                    {'error', 'timeout'}
            end;
        'undefined' ->
            {'error', 'noproc'};
        {'error', _} = Error ->
            Error
    end;
stop_scope({Type, _} = ScopeID, 'infinity') when erlang:is_atom(Type) ->
    case lookup(?SCOPE_SUP_ID(ScopeID)) of
        Sup when erlang:is_pid(Sup) ->
            % Use monitor+cast so we only have one handler in the server, even
            % though this could be done fully synchronously. See above for why.
            % TODO: This is inherently dangerous, as the cast is unreliable.
            % Without a timeout, if the manager process were to go away between
            % the lookup and now, this function would never return unless the
            % supervisor stopped for some other reason. The "right way" would
            % be to have a synchronous protocol initiating an async shutdown in
            % yet another process and returning the supervisor pid to be
            % monitored here only when the shutdown is known to have started.
            Ref = erlang:monitor('process', Sup),
            gen_server:cast(?SERVICE_NAME, {'stop_scope', ScopeID}),
            receive
                {'DOWN', Ref, _, Sup, _} ->
                    'ok'
            end;
        'undefined' ->
            {'error', 'noproc'};
        {'error', _} = Error ->
            Error
    end.

-spec job_svc(scope_svc_id() | scope_id()) -> pid() | {'error', term()}.
%%
%% @doc So much like start_scope/1 that it may not be worth keeping it.
%%
job_svc(?SCOPE_SVC_ID({Type, _}) = SvcId) when erlang:is_atom(Type) ->
    case gen_server:call(?SERVICE_NAME,
            {'job_svc', SvcId}, ?SCOPE_SVC_STARTUP_TIMEOUT) of
        'lookup' ->
            lookup(SvcId);
        Ret ->
            Ret
    end;
job_svc({Type, _} = ScopeID) when erlang:is_atom(Type) ->
    job_svc(?SCOPE_SVC_ID(ScopeID)).

-spec lookup(proc_id()) -> pid() | 'undefined' | {'error', term()}.
%%
%% @doc Find the pid of the specified process.
%%
%% Unlike job_svc/1, this NEVER starts the process.
%%
lookup({PType, {NType, _}} = Id)
        when erlang:is_atom(PType) andalso erlang:is_atom(NType) ->
    gen_server:call(?SERVICE_NAME, {'lookup', Id}).

-spec group(scope_id()) -> [{proc_id(), pid()}].
%%
%% @doc Find the running (non-runner) processes for a vnode.
%%
group({NType, _} = ScopeID) when erlang:is_atom(NType) ->
    gen_server:call(?SERVICE_NAME, {'group', ScopeID}).

-spec group(proc_type(), scope_type()) -> [{proc_id(), pid()}].
%%
%% @doc Find the running processes of a specified type for a vnode type.
%%
group(PType, NType) when erlang:is_atom(PType) andalso erlang:is_atom(NType) ->
    gen_server:call(?SERVICE_NAME, {'group', PType, NType}).

-spec register(proc_id(), pid()) -> 'ok'.
%%
%% @doc Register the specified process.
%%
register({PType, {NType, _}} = Id, Pid) when erlang:is_pid(Pid)
        andalso erlang:is_atom(PType) andalso erlang:is_atom(NType) ->
    gen_server:cast(?SERVICE_NAME, {'register', Id, Pid}).

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
handle_call({'lookup', Id}, _, #state{pdict = D} = State) ->
    case pdict_find(Id, D) of
        #prec{pid = Pid} ->
            {'reply', Pid, State};
        'false' ->
            {'reply', 'undefined', State}
    end;
%
% group(scope_id()) -> [{proc_id(), pid()}]
%
handle_call({'group', ScopeID}, _, #state{pdict = D} = State) ->
    Ret = [{{T, {N, I}}, P}
            || #prec{ptype = T, stype = N, six = I, pid = P}
                <- pdict_group(ScopeID, D)],
    {'reply', Ret, State};
%
% group(proc_type(), scope_type()) -> [{proc_id(), pid()}]
%
handle_call({'group', PType, NType}, _, #state{pdict = D} = State) ->
    Ret = [{{T, {N, I}}, P}
            || #prec{ptype = T, stype = N, six = I, pid = P}
                <- pdict_group(PType, NType, D)],
    {'reply', Ret, State};
%
% job_svc(node_mgr_id()) -> pid()
%
handle_call({'job_svc', ?SCOPE_SVC_ID({T, _} = ScopeID) = SvcId}, _,
        #state{jobs_sup = S, pdict = D, cdict = C, dummy = J} = State) ->
    case pdict_find(SvcId, D) of
        #prec{pid = Pid} ->
            {'reply', Pid, State};
        _ ->
            Config = case lists:keyfind(T, 1, C) of
                'false' ->
                    [];
                {_, Cfg} ->
                    Cfg
            end,
            case riak_core_job_sup:start_scope(S, ScopeID, Config, J) of
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
handle_call({'start_scope', {T, _} = ScopeID}, _,
        #state{jobs_sup = S, pdict = D, cdict = C, dummy = J} = State) ->
    case pdict_find(?SCOPE_SVC_ID(ScopeID), D) of
        #prec{} ->
            {'reply', 'ok', State};
        _ ->
            Config = case lists:keyfind(T, 1, C) of
                'false' ->
                    [];
                {_, Cfg} ->
                    Cfg
            end,
            case riak_core_job_sup:start_scope(S, ScopeID, Config, J) of
                {'ok', _} ->
                    {'reply', 'ok', State};
                {'error', _} = Err ->
                    {'reply', Err, State}
            end
    end;
%
% start_scope(scope_id(), riak_core_job_service:config()) -> 'ok' | {'error', term()}
%
handle_call({'start_scope', {T, _} = ScopeID, Config}, _,
        #state{jobs_sup = S, pdict = D, cdict = C, dummy = J} = State) ->
    case pdict_find(?SCOPE_SVC_ID(ScopeID), D) of
        #prec{} ->
            {'reply', 'ok', State};
        _ ->
            case riak_core_job_sup:start_scope(S, ScopeID, Config, J) of
                {ok, _} ->
                    case lists:keyfind(T, 1, C) of
                        'false' ->
                            {'reply', 'ok',
                                State#state{cdict = [{T, Config} | C]}};
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
                            {'reply', 'ok', State#state{
                                cdict = lists:keystore(T, 1, C, {T, Config})}}
                    end;
                {'error', _} = Err ->
                    {'reply', Err, State}
            end
    end;
%
% unrecognized message
%
handle_call(Msg, From, #state{svc_name = N} = State) ->
    _ = lager:error(
            "~p service received unhandled call from ~p: ~p", [N, From, Msg]),
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
handle_cast({'register', ?WORK_SUP_ID(ScopeID) = Id, Sup}, StateIn) ->
    State = StateIn#state{pdict = pdict_store(Id, Sup, StateIn#state.pdict)},
    case pdict_find(?SCOPE_SVC_ID(ScopeID), State#state.pdict) of
        #prec{pid = Svc} ->
            _ = riak_core_job_service:register(Svc, Id, Sup),
            {'noreply', State};
        _ ->
            % in case things are getting scheduled weird, retry a few times
            _ = ?cast({'retry_work_reg', 5, Sup}),
            {'noreply', State}
    end;
%
% register(proc_id(), pid()) -> 'ok'
%
handle_cast({'register', Id, Pid}, #state{pdict = D} = State) ->
    {'noreply', State#state{pdict = pdict_store(Id, Pid, D)}};
%
% special message to retry registering a work supervisor with its service
% confirm the supervisor's pid to make sure it's still registered itself
%
handle_cast({'retry_work_reg', Count, ?WORK_SUP_ID(ScopeID) = Id, Sup},
        #state{svc_name = N, pdict = D} = State) ->
    case pdict_find(Id, D) of
        #prec{pid = Sup} ->
            case pdict_find(?SCOPE_SVC_ID(ScopeID), D) of
                #prec{pid = Svc} ->
                    _ = riak_core_job_service:register(Svc, Id, Sup),
                    {'noreply', State};
                _ ->
                    C = (Count - 1),
                    case C > 0 of
                        'true' ->
                            _ = ?cast({'retry_work_reg', C, Sup}),
                            {'noreply', State};
                        _ ->
                            _ = lager:error(
                                    "~p service stranded ~p: ~p", [N, Id, Sup]),
                            {'noreply', State}
                    end
            end;
        _ ->
            {'noreply', State}
    end;
%
% stop_scope(scope_id()) -> 'ok'
% See the lengthy comment in stop_scope/2 about how this really should be done.
%
handle_cast({'stop_scope', ScopeID},
        #state{svc_name = N, jobs_sup = S} = State) ->
    case riak_core_job_sup:stop_scope(S, ScopeID) of
        ok ->
            {'noreply', State};
        {'error', Err} ->
            _ = lager:error(
                    "~p service error ~p stopping scope ~p", [N, Err, ScopeID]),
            {'noreply', State}
    end;
%
% placed here once by init/2 at startup
% we'll never see this again, so it's the last pattern to handle
%
handle_cast('init', #state{jobs_sup = S, cdict = C, pdict = D} = State) ->
    {CD, PD} = absorb_sup_tree(supervisor:which_children(S), {C, D}),
    {'noreply', State#state{cdict = CD, pdict = PD}};
%
% unrecognized message
%
handle_cast(Msg, #state{svc_name = N} = State) ->
    _ = lager:error("~p service received unhandled cast: ~p", [N, Msg]),
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
handle_info({'DOWN', Mon, _, _, _}, #state{pdict = D} = State) ->
    {'noreply', State#state{pdict = pdict_erase(Mon, D)}};
%
% unrecognized message
%
handle_info(Msg, #state{svc_name = N} = State) ->
    _ = lager:error("~p service received unhandled info: ~p", [N, Msg]),
    {'noreply', State}.

-spec terminate(term(), state()) -> ok.
%
% no matter why we're terminating, de-monitor everything
%
terminate(_, #state{pdict = D}) ->
    pdict_demonitor(D).

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

absorb_sup_tree([{?SCOPE_SVC_ID({N, _}) = Id, Pid, _, _} | Rest], {CDIn, PD}) ->
    % See if we can grab a config we don't already have. There's no intelligent
    % way to get the latest one in the current situation, so take the first of
    % each type.
    CD = case lists:keyfind(N, 1, CDIn) of
        'false' ->
            case riak_core_job_service:config(Pid) of
                [_|_] = Config ->
                    [{N, Config} | CDIn];
                _ ->
                    CDIn
            end;
        _ ->
            CDIn
    end,
    absorb_sup_tree(Rest, {CD, pdict_store(Id, Pid, PD)});

absorb_sup_tree([{?WORK_SUP_ID(_) = Id, Pid, _, _} | Rest], {CD, PD}) ->
    % Don't descend into work runner supervisors. It would be preferable to
    % be able to check the supervisor's restart strategy, but we can't get to
    % that through the public API.
    absorb_sup_tree(Rest, {CD, pdict_store(Id, Pid, PD)});

absorb_sup_tree([{Id, Pid, 'supervisor', _} | Rest], {CD, PD}) ->
    absorb_sup_tree(Rest, absorb_sup_tree(
        supervisor:which_children(Pid), {CD, pdict_store(Id, Pid, PD)}));

absorb_sup_tree([{Id, Pid, _, _} | Rest], {CD, PD}) ->
    absorb_sup_tree(Rest, {CD, pdict_store(Id, Pid, PD)});

absorb_sup_tree([], Dicts) ->
    Dicts.

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
% TODO: Implement this with maps or dicts?
%
-type pdict() :: [prec()].

pdict_new() ->
    [].

pdict_demonitor([#prec{mon = M} | D]) ->
    _ = erlang:demonitor(M, ['flush']),
    pdict_demonitor(D);
pdict_demonitor([]) ->
    'ok'.

pdict_erase(#prec{mon = M}, D) ->
    pdict_erase(M, D);

pdict_erase(M, D) when erlang:is_reference(M) ->
    _ = erlang:demonitor(M, ['flush']),
    case lists:keytake(M, #prec.mon, D) of
        {'value', _, N} ->
            N;
        'false' ->
            D
    end;

pdict_erase(I, D) ->
    pdict_erase(I, D, []).

pdict_erase({T, {N, I}},
        [#prec{ptype = T, stype = N, six = I, mon = M} | A], B) ->
    _ = erlang:demonitor(M, ['flush']),
    B ++ A;
pdict_erase(I, [R | A], B) ->
    pdict_erase(I, A, [R | B]);
pdict_erase(_, [], B) ->
    B.

pdict_find(M, D) when erlang:is_reference(M) ->
    lists:keyfind(M, #prec.mon, D);

pdict_find({T, {N, I}}, [#prec{ptype = T, stype = N, six = I} = R | _]) ->
    R;
pdict_find(I, [_ | D]) ->
    pdict_find(I, D);
pdict_find(_, []) ->
    'false'.

pdict_group(K, D) ->
    pdict_group_scan(K, D, []).

pdict_group(T, N, D) ->
    pdict_group_scan(T, N, D, []).

pdict_group_scan({N, I} = K, [#prec{stype = N, six = I} = R | D], A) ->
    pdict_group_scan(K, D, [R | A]);
pdict_group_scan(K, [_ | D], A) ->
    pdict_group_scan(K, D, A);
pdict_group_scan(_, [], A) ->
    A.

pdict_group_scan(T, N, [#prec{ptype = T, stype = N} = R | D], A) ->
    pdict_group_scan(T, N, D, [R | A]);
pdict_group_scan(T, N, [_ | D], A) ->
    pdict_group_scan(T, N, D, A);
pdict_group_scan(_, _, [], A) ->
    A.

-compile([{nowarn_unused_function, {pdict_store, 2}}]).
pdict_store(#prec{ptype = T, stype = N, six = I} = R, D) ->
    case pdict_find({T, {N, I}}, D) of
        'false' ->
            [R | D];
        R ->
            D;
        _ ->
            pdict_replace(R, D, [])
    end.

pdict_store({T, {N, I}} = K, P, D) when erlang:is_pid(P) ->
    case pdict_find(K, D) of
        'false' ->
            [#prec{ptype = T, stype = N, six = I, pid = P,
                mon = erlang:monitor('process', P)} | D];
        #prec{ptype = T, stype = N, six = I, pid = P} ->
            D;
        _ ->
            pdict_replace(#prec{ptype = T, stype = N, six = I, pid = P,
                mon = erlang:monitor('process', P)}, D, [])
    end.

pdict_replace(#prec{ptype = T, stype = N, six = I, mon = M} = R,
        [#prec{ptype = T, stype = N, six = I, mon = M} | A], B) ->
    B ++ [R | A];
pdict_replace(#prec{ptype = T, stype = N, six = I} = R,
        [#prec{ptype = T, stype = N, six = I, mon = O} | A], B) ->
    _ = erlang:demonitor(O, ['flush']),
    B ++ [R | A];
pdict_replace(N, [R | A], B) ->
    pdict_replace(N, A, [R | B]);
pdict_replace(N, [], B) ->
    [N | B].

-endif.

