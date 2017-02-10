-module(riak_core_info_service_process).


-export([start_link/3]).

%% Application messages are sent to this process directly from
%% NIF-land instead of other Erlang modules, so this does not
%% implement an OTP behavior.
%%
%% See http://erlang.org/doc/design_principles/spec_proc.html for
%% information on these system message handlers.
-export([system_continue/3, system_terminate/4,
         system_get_state/1, system_replace_state/2,
         system_code_change/4]).



start_link(Registration, Source, Handler) ->
 
    Pid = proc_lib:spawn_link(?MODULE, callback_router,
                              [Source, Handler, self(), sys:debug_options([])]),
    
    %% Inform the NIF where to send messages
    apply_callback(Registration, [Pid]),
    {ok, Pid}.

apply_callback({Mod, Fun, InitialArgs}, RestArgs) ->
    %% Inform the NIF where to send messages
    erlang:apply(Mod, Fun, InitialArgs ++ RestArgs).



-spec callback_router(riak_core_info_service:callback(), 
                      riak_core_info_service:callback(), 
                      pid(), term()) -> ok.
callback_router(Source, Handler, Parent, Debug) ->
    receive
        {get, SourceParams, HandlerContext}=Msg ->
            Debug0 = sys:handle_debug(Debug, fun write_debug/3,
                                      ?MODULE,
                                      {in, Msg}),
            Result = apply_callback(Source, SourceParams),
            apply_callback(Handler, [{Result, SourceParams, HandlerContext}]), 
            callback_router(Source, Handler, Parent, Debug0);

        {system, From, Request} ->
            %% No recursion here; `system_continue/3' will be invoked
            %% if appropriate
            sys:handle_system_msg(Request, From, Parent,
                                  ?MODULE, Debug, {Source, Handler});

        callback_shutdown=Msg ->
            sys:handle_debug(Debug, fun write_debug/3,
                             ?MODULE,
                             {in, Msg}),
            ok;

        Msg ->
            Debug0 = sys:handle_debug(Debug, fun write_debug/3,
                                      ?MODULE,
                                      {in, Msg}),
            callback_router(Source, Handler, Parent, Debug0)
    end.

%% Made visible to `standard_io' via `sys:trace(Pid, true)'
write_debug(Device, Event, Module) ->
    io:format(Device, "~p event: ~p~n", [Module, Event]).

%% System functions invoked by `sys:handle_system_msg'.
system_continue(Parent, Debug, Fun) ->
    callback_router(Fun, Parent, Debug).

system_terminate(Reason, _Parent, _Debug, _State) ->
    exit(Reason).

system_get_state(State) ->
    {ok, State}.

system_replace_state(StateFun, State) ->
    NewState = StateFun(State),
    {ok, NewState, NewState}.

system_code_change(State, _Module, _OldVsn, _Extra) ->
    {ok, State}.

