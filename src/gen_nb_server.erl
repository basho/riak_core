%% Copyright (c) 2009 Hypothetical Labs, Inc.

%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%%
%% The above copyright notice and this permission notice shall be included in
%% all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
%% THE SOFTWARE.

-module(gen_nb_server).

-author('kevin@hypotheticalabs.com').

-behaviour(gen_server).

%% API
-export([start_link/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {cb,
                sock,
                server_state}).

-callback init(InitArgs::list()) -> 
    {ok, State::term()} | 
    {error, Reason::term()}.

-callback handle_call(Msg::term(), From::{pid(), term()}, State::term()) -> 
    {reply, Reply::term(), State::term()} | 
    {reply, Reply::term(), State::term(), number() | hibernate} | 
    {noreply, State::term()} | 
    {noreply, State::term(), number() | hibernate} | 
    {stop, Reason::term(), State::term()}.

-callback handle_cast(Msg::term(), State::term()) ->
    {noreply, State::term()} | 
    {noreply, State::term(), number() | hibernate} | 
    {stop, Reason::term(), State::term()}.

-callback handle_info(Msg::term(), State::term()) ->
    {noreply, State::term()} | 
    {noreply, State::term(), number() | hibernate} | 
    {stop, Reason::term(), State::term()}.

-callback terminate(Reason::term(), State::term()) ->    
    ok.

-callback sock_opts() -> [gen_tcp:listen_option()].

-callback new_connection(inet:socket(), State::term()) ->
    {ok, NewState::term()} | 
    {stop, Reason::term(), NewState::term()}.

%% @spec start_link(CallbackModule, IpAddr, Port, InitParams) -> Result
%% CallbackModule = atom()
%% IpAddr = string()
%% Port = integer()
%% InitParams = [any()]
%% Result = {ok, pid()} | {error, any()}
%% @doc Start server listening on IpAddr:Port
start_link(CallbackModule, IpAddr, Port, InitParams) ->
  gen_server:start_link(?MODULE, [CallbackModule, IpAddr, Port, InitParams], []).

%% @hidden
init([CallbackModule, IpAddr, Port, InitParams]) ->
  case CallbackModule:init(InitParams) of
    {ok, ServerState} ->
      case listen_on(CallbackModule, IpAddr, Port) of
        {ok, Sock} ->
          {ok, #state{cb=CallbackModule, sock=Sock, server_state=ServerState}};
        Error ->
          CallbackModule:terminate(Error, ServerState),
          Error
      end;
    Err ->
      Err
  end.

%% @hidden
handle_call(Request, From, #state{cb=Callback, server_state=ServerState}=State) ->
  case Callback:handle_call(Request, From, ServerState) of
    {reply, Reply, NewServerState} ->
      {reply, Reply, State#state{server_state=NewServerState}};
    {reply, Reply, NewServerState, Arg} when Arg =:= hibernate orelse is_number(Arg) ->
      {reply, Reply, State#state{server_state=NewServerState}, Arg};
    {noreply, NewServerState} ->
      {noreply, State#state{server_state=NewServerState}};
    {noreply, NewServerState, Arg} when Arg =:= hibernate orelse is_number(Arg) ->
      {noreply, State#state{server_state=NewServerState}, Arg};
    {stop, Reason, NewServerState} ->
      {stop, Reason, State#state{server_state=NewServerState}};
    {stop, Reason, Reply, NewServerState} ->
      {stop, Reason, Reply, State#state{server_state=NewServerState}}
  end.

%% @hidden
handle_cast(Msg, #state{cb=Callback, server_state=ServerState}=State) ->
  case Callback:handle_cast(Msg, ServerState) of
    {noreply, NewServerState} ->
      {noreply, State#state{server_state=NewServerState}};
    {noreply, NewServerState, Arg} when Arg =:= hibernate orelse is_number(Arg) ->
      {noreply, State#state{server_state=NewServerState}, Arg};
    {stop, Reason, NewServerState} ->
      {stop, Reason, State#state{server_state=NewServerState}}
  end.

%% @hidden
handle_info({inet_async, ListSock, _Ref, {ok, CliSocket}}, #state{cb=Callback, server_state=ServerState}=State) ->
  inet_db:register_socket(CliSocket, inet_tcp),
  case Callback:new_connection(CliSocket, ServerState) of
    {ok, NewServerState} ->
      {ok, _} = prim_inet:async_accept(ListSock, -1),
      {noreply, State#state{server_state=NewServerState}};
    {stop, Reason, NewServerState} ->
      {stop, Reason, State#state{server_state=NewServerState}}
  end;

handle_info(Info, #state{cb=Callback, server_state=ServerState}=State) ->
  case Callback:handle_info(Info, ServerState) of
    {noreply, NewServerState} ->
      {noreply, State#state{server_state=NewServerState}};
    {noreply, NewServerState, Arg} when Arg =:= hibernate orelse is_number(Arg) ->
      {noreply, State#state{server_state=NewServerState}, Arg};
    {stop, Reason, NewServerState} ->
      {stop, Reason, State#state{server_state=NewServerState}}
  end.

%% @hidden
terminate(Reason, #state{cb=Callback, sock=Sock, server_state=ServerState}) ->
  gen_tcp:close(Sock),
  Callback:terminate(Reason, ServerState),
  ok.

%% @hidden
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%% Internal functions

%% @hidden
%% @spec listen_on(CallbackModule, IpAddr, Port) -> Result
%% CallbackModule = atom()
%% IpAddr = string() | tuple()
%% Port = integer()
%% Result = {ok, port()} | {error, any()}
listen_on(CallbackModule, IpAddr, Port) when is_tuple(IpAddr) andalso
                                             (8 =:= size(IpAddr) orelse
                                              4 =:= size(IpAddr)) ->
    SockOpts = [{ip, IpAddr}|CallbackModule:sock_opts()],
    case gen_tcp:listen(Port, SockOpts) of
        {ok, LSock} ->
            {ok, _Ref} = prim_inet:async_accept(LSock, -1),
            {ok, LSock};
        Err ->
            Err
    end;
listen_on(CallbackModule, IpAddrStr, Port) ->
    case inet_parse:address(IpAddrStr) of
        {ok, IpAddr} ->
            listen_on(CallbackModule, IpAddr, Port);
        Err ->
            lager:critical("Cannot start listener for ~p on invalid address ~p:~p", [CallbackModule, IpAddrStr, Port]),
            Err
    end.
