-module(riak_core_info_service_process).

-record(state, {
    source = undefined :: riak_core_info_service:callback(),
    handler = undefined :: riak_core_info_service:callback(),
    shutdown = undefined :: riak_core_info_service:callback(),
    parent :: pid(),
    debug :: list()
}).

-export([start_link/4]).

%% Application messages are sent to this process directly from
%% NIF-land instead of other Erlang modules, so this does not
%% implement an OTP behavior.
%%
%% See http://erlang.org/doc/design_principles/spec_proc.html for
%% information on these system message handlers.
-export([system_continue/3, system_terminate/4,
         system_get_state/1, system_replace_state/2,
         system_code_change/4, callback_router/1]).

start_link(Registration, Shutdown, Source, Handler) ->
    State = #state{shutdown = Shutdown,
        source = Source,
        handler = Handler,
        parent = self(),
        debug = sys:debug_options([])},
    Pid = proc_lib:spawn_link(?MODULE, callback_router,
                              [State]),
    
    %% Call the Registration function provided to let the registered system
    %% know the Pid of its info process
    apply_callback(Registration, [Pid]),
    {ok, Pid}.

apply_callback(undefined, _CallSpecificArgs) ->
    ok;
apply_callback({Mod, Fun, StaticArgs}, CallSpecificArgs) ->
    %% Invoke the callback, passing static + call-specific args for this call
    erlang:apply(Mod, Fun, StaticArgs ++ CallSpecificArgs).

-spec callback_router(#state{}) -> ok.
callback_router(#state{} = State) ->
    receive
        {invoke, SourceParams, HandlerContext}=Msg ->
            handle_invoke_message(Msg, SourceParams, HandlerContext, State);
        {system, From, Request} ->
            handle_system_message(Request, From, State);
        callback_shutdown=Msg ->
            handle_shutdown_message(Msg, State);
        Msg ->
            handle_other_message(Msg, State)
    end.

handle_invoke_message(Msg, SourceParams, HandlerContext,
                   #state{ debug = Debug0,
                           source = Source,
                           handler = Handler }=State) ->
    Debug = sys:handle_debug(Debug0, fun write_debug/3,
                             ?MODULE,
                             {in, Msg}),
    Result = apply_callback(Source, SourceParams),
    apply_callback(Handler, [{Result, SourceParams, HandlerContext}]),
    callback_router(State#state{debug = Debug}).

handle_shutdown_message(Msg, #state{debug = Debug, shutdown = Shutdown}) ->
    io:format(user,"handle_shutdown_message~n", []),
    sys:handle_debug(Debug, fun write_debug/3,
                     ?MODULE,
                     {in, Msg}),
    apply_callback(Shutdown, [self()]),
    ok.

handle_system_message(Request, From, #state{ debug=Debug, parent = Parent } = State) ->
    %% No recursion here; `system_continue/3' will be invoked
    %% if appropriate
    sys:handle_system_msg(Request, From, Parent,
                          ?MODULE, Debug, State),
    ok.

handle_other_message(Msg, #state{debug=Debug0} = State) ->
    Debug = sys:handle_debug(Debug0, fun write_debug/3,
                              ?MODULE,
                              {in, Msg}),
    callback_router(State#state{debug = Debug}).


%% Made visible to `standard_io' via `sys:trace(Pid, true)'
write_debug(Device, Event, Module) ->
    io:format(Device, "~p event: ~p~n", [Module, Event]).

%% System functions invoked by `sys:handle_system_msg'.
system_continue(_Parent, Debug, State) ->
    callback_router(State#state{debug = Debug}).

system_terminate(Reason, _Parent, _Debug, _State) ->
    exit(Reason).

system_get_state(State) ->
    {ok, State}.

system_replace_state(StateFun, State) ->
    NewState = StateFun(State),
    {ok, NewState, NewState}.

system_code_change(State, _Module, _OldVsn, _Extra) ->
    {ok, State}.

