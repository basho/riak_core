-module(riak_core_info_service_process).


-export([start_link/4]).

%% Application messages are sent to this process directly from
%% NIF-land instead of other Erlang modules, so this does not
%% implement an OTP behavior.
%%
%% See http://erlang.org/doc/design_principles/spec_proc.html for
%% information on these system message handlers.
-export([system_continue/3, system_terminate/4,
         system_get_state/1, system_replace_state/2,
         system_code_change/4, callback_router/5]).

start_link(Registration, Shutdown, Source, Handler) ->
    Pid = proc_lib:spawn_link(?MODULE, callback_router,
                              [Shutdown, Source, Handler, self(), sys:debug_options([])]),
    
    %% Call the Registration function provided to let the registered syste
    %% know the Pid of its info process
    apply_callback(Registration, [Pid]),
    {ok, Pid}.

apply_callback(undefined, _CallSpecificArgs) ->
    ok;
apply_callback({Mod, Fun, StaticArgs}, CallSpecificArgs) ->
    %% Invoke the callback, passing static + call-specific args for this call
    erlang:apply(Mod, Fun, StaticArgs ++ CallSpecificArgs).

-spec callback_router(Shutdown::riak_core_info_service:callback(),
                      Source::riak_core_info_service:callback(),
                      Handler::riak_core_info_service:callback(),
                      pid(), term()) -> ok.
callback_router(Shutdown, Source, Handler, Parent, Debug) ->
    receive
        {invoke, SourceParams, HandlerContext}=Msg ->
            handle_get_message(Debug, Msg, Source, SourceParams, Handler, HandlerContext, Parent, Shutdown);
        {system, From, Request} ->
            handle_system_message(Request, From, Parent, Debug, Source, Handler, Shutdown);
        callback_shutdown=Msg ->
            handle_shutdown_message(Debug, Msg, Shutdown);
        Msg ->
            handle_other_message(Debug, Msg, Source, Handler, Parent, Shutdown)
    end.

handle_shutdown_message(Debug, Msg, Shutdown) ->
    sys:handle_debug(Debug, fun write_debug/3,
                     ?MODULE,
                     {in, Msg}),
    apply_callback(Shutdown, [self()]),
    ok.

handle_system_message(Request, From, Parent, Debug, Source, Handler, Shutdown) ->
    %% No recursion here; `system_continue/3' will be invoked
    %% if appropriate
    sys:handle_system_msg(Request, From, Parent,
                          ?MODULE, Debug, {Source, Handler, Shutdown}),
    ok.

handle_get_message(Debug0, Msg, Source, SourceParams, Handler, HandlerContext, Parent, Shutdown) ->
    Debug = sys:handle_debug(Debug0, fun write_debug/3,
                              ?MODULE,
                              {in, Msg}),
    Result = apply_callback(Source, SourceParams),
    apply_callback(Handler, [{Result, SourceParams, HandlerContext}]),
    callback_router(Source, Handler, Parent, Debug, Shutdown).

handle_other_message(Debug, Msg, Source, Handler, Parent, Shutdown) ->
    Debug0 = sys:handle_debug(Debug, fun write_debug/3,
                              ?MODULE,
                              {in, Msg}),
    callback_router(Source, Handler, Parent, Debug0, Shutdown).


%% Made visible to `standard_io' via `sys:trace(Pid, true)'
write_debug(Device, Event, Module) ->
    io:format(Device, "~p event: ~p~n", [Module, Event]).

%% System functions invoked by `sys:handle_system_msg'.
system_continue(Parent, Debug, {Source, Handler, Shutdown}) ->
    callback_router(Shutdown, Source, Handler, Parent, Debug).

system_terminate(Reason, _Parent, _Debug, _State) ->
    exit(Reason).

system_get_state(State) ->
    {ok, State}.

system_replace_state(StateFun, State) ->
    NewState = StateFun(State),
    {ok, NewState, NewState}.

system_code_change(State, _Module, _OldVsn, _Extra) ->
    {ok, State}.

