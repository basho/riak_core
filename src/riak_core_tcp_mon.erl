%% -------------------------------------------------------------------
%%
%% TCP Connection Monitor
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


-module(riak_core_tcp_mon).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/0, start_link/1, monitor/3, status/0, status/1, format/0, format/2]).
-export([default_status_funs/0, raw/2, diff/2, rate/2, kbps/2,
         socket_status/1, format_socket_stats/2 ]).

%% gen_server callbacks
-behavior(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% Keep 6 x 10s worth of data plus an extra sample for working out rates
-define(DEFAULT_LIMIT, 7).
-define(DEFAULT_INTERVAL, timer:seconds(10)).
-define(DEFAULT_CLEAR, timer:seconds(60)).

-define(INET_STATS, [recv_oct,recv_cnt,recv_max,recv_avg,recv_dvi,
                     send_oct,send_cnt,send_max,send_avg,send_pend]).
-define(INET_OPTS, [sndbuf,recbuf,active,buffer]).

-define(STATUS_FUNS, [{recv_oct, {recv_kbps, fun kbps/2}}, {recv_cnt, fun diff/2},
                      {recv_max, fun raw/2}, {recv_avg, fun raw/2}, {recv_dvi, fun raw/2},
                      {send_oct, {send_kbps, fun kbps/2}}, {send_cnt, fun diff/2},
                      {send_max, fun raw/2}, {send_avg, fun raw/2}, {send_pend, fun raw/2},
                      {sndbuf, fun raw/2}, {recbuf, fun raw/2}, {active, fun raw/2},
                      {buffer, fun raw/2}]).

-record(state, {conns = gb_trees:empty(),      % conn records keyed by Socket
                tags = gb_trees:empty(),       % tags to ports
                interval = ?DEFAULT_INTERVAL,  % how often to get stats
                limit = ?DEFAULT_LIMIT,        % 
                clear_after = ?DEFAULT_CLEAR,  % how long to leave errored sockets in status
                stats = ?INET_STATS,           % Stats to read
                opts  = ?INET_OPTS,            % Opts to read
                status_funs = dict:from_list(default_status_funs())  % Status reporting functions
                }).

-record(conn, {tag,               %% Tag used to find socket
               transport,
               type,              %% Type - normal, dist, error
               ts_hist = [],      %% History of timestamps for readings
               hist = []}).       %% History of readings


start_link() ->
    start_link([]).

start_link(Props) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Props, []).

monitor(Socket, Tag, Transport) ->
    gen_server:call(?MODULE, {monitor, Socket, Tag, Transport}).

status() ->
    gen_server:call(?MODULE, status).

status(Timeout) ->
    gen_server:call(?MODULE, status, Timeout).

socket_status(Socket) ->
  gen_server:call(?MODULE, {socket_status, Socket}).

format() ->
    Status = status(),
    io:fwrite([format(Status, recv_kbps),
              format(Status, send_kbps)]).

format(Status, Stat) ->
    [format_header(Stat),
     [format_entry(Entry, Stat) || Entry <- Status]].

format_header(Stat) ->
    io_lib:format("~40w Value\n", [Stat]).

format_entry(Status, Stat) ->
    Tag = proplists:get_value(tag, Status),
    Value = proplists:get_value(Stat, Status),
    case Value of
        Value when is_list(Value) ->
            [format_tag(Tag),
             " ",
             format_list(Value),
             "\n"];
        _ ->
            [format_tag(Tag),
             " [",
             format_value(Value),
             "\n"]
    end.

format_tag(Tag) when is_list(Tag) ->
    io_lib:format("~40s", [Tag]);
format_tag(Tag) ->
    io_lib:format("~40w", [Tag]).

format_value(Val) when is_float(Val) ->
    io_lib:format("~7.1f", [Val]);
format_value(Val) ->
    io_lib:format("~w", [Val]).

format_list(Value) ->
    [$[, string:join([format_value(Item) || Item <- Value], ", "), $]].

%% Provide a way to get to the default status fun
default_status_funs() ->
    ?STATUS_FUNS.

%% Return raw readings, ignore timestamps
raw(_TS, Hist) ->
    Hist.

diff(TS, Hist) ->
    RevTS = lists:reverse(TS),
    RevHist = lists:reverse(Hist),
    diff(RevTS, RevHist, []).    

diff([_TS], [_C], Acc) ->
    Acc;
diff([_TS1 | TSRest], [C1 | CRest], Acc) ->
    Diff = hd(CRest) - C1,
    diff(TSRest, CRest, [Diff | Acc]).

%% Convert byte rate to bit rate
kbps(TS, Hist) ->
    [trunc(R / 128.0) || R <- rate(TS, Hist)]. %  *8 bits / 1024 bytes

%% Work out the rate of something per second
rate(TS, Hist) ->
    RevTS = lists:reverse(TS),
    RevHist = lists:reverse(Hist),
    rate(RevTS, RevHist, []).

rate([_TS], [_C], Acc) ->
    Acc;
rate([TS1 | TSRest], [C1 | CRest], Acc) ->
    Secs = timer:now_diff(hd(TSRest), TS1) / 1.0e6,
    Rate = (hd(CRest) - C1) / Secs,
    rate(TSRest, CRest, [Rate | Acc]).

init(Props) ->
    lager:info("Starting TCP Monitor"),
    ok = net_kernel:monitor_nodes(true, [{node_type, visible}, nodedown_reason]),
    State0 = #state{interval = proplists:get_value(interval, Props, ?DEFAULT_INTERVAL),
                    limit = proplists:get_value(limit, Props, ?DEFAULT_LIMIT),
                    clear_after = proplists:get_value(clear_after, Props, ?DEFAULT_LIMIT)},
    DistCtrl = erlang:system_info(dist_ctrl),
    State = lists:foldl(fun({Node,Port}, DatState) ->
            add_dist_conn(Node, Port, DatState)
    end, State0, DistCtrl),
   {ok, schedule_tick(State)}.

handle_call(status, _From, State = #state{conns = Conns,
                                          status_funs = StatusFuns}) ->
    Out = [ [{socket,P} | conn_status(P, Conn, StatusFuns)]
                || {P,Conn} <- gb_trees:to_list(Conns)],
    {reply, Out , State};

handle_call({socket_status, Socket}, _From, State = #state{conns = Conns,
                                          status_funs = StatusFuns}) ->
    Stats =
        case gb_trees:lookup(Socket, Conns) of
          none -> [];
        {value, Conn} -> conn_status(Socket, Conn, StatusFuns)
        end,
    {reply, Stats, State};

handle_call({monitor, Socket, Tag, Transport}, _From, State) ->
    {reply, ok,  add_conn(Socket, #conn{tag = Tag, type = normal,
                                        transport = Transport}, State)}.

handle_cast(Msg, State) ->
    lager:warning("unknown message received: ~p", [Msg]),
    {noreply, State}.

handle_info({nodeup, Node, _InfoList}, State) ->
    DistCtrl = erlang:system_info(dist_ctrl),
    case proplists:get_value(Node, DistCtrl) of
        undefined ->
            lager:error("Could not get dist for ~p\n~p\n", [Node, DistCtrl]),
            {noreply, State};
        Port ->
            {noreply, add_dist_conn(Node, Port, State)}
    end;

handle_info({nodedown, Node, _InfoList}, State) ->
    GbList = gb_trees:to_list(State#state.conns),
    MaybePortConn = [{P, C} ||
        {P, #conn{type = dist, tag = {node, MaybeNode}} = C} <- GbList,
        MaybeNode =:= Node],
    Conns2 = case MaybePortConn of
        [{Port, Conn} | _] ->
            erlang:send_after(State#state.clear_after, self(), {clear, Port}),
            Conn2 = Conn#conn{type = error},
            gb_trees:update(Port, Conn2, State#state.conns);
        _ ->
            State#state.conns
    end,
    {noreply, State#state{conns = Conns2}};

handle_info(measurement_tick, State = #state{limit = Limit, stats = Stats,
                                             opts = Opts, conns = Conns}) ->
    schedule_tick(State),
    Fun = fun(Socket, Conn = #conn{type = Type, ts_hist = TSHist, hist = Hist}) when Type /= error ->
                  try
                      {ok, StatVals} = inet:getstat(Socket, Stats),
                      TS = os:timestamp(), % read between the two split the difference
                      {ok, OptVals} = inet:getopts(Socket, Opts),
                      Hist2 = update_hist(OptVals, Limit,
                                          update_hist(StatVals, Limit, Hist)),
                      Conn#conn{ts_hist = prepend_trunc(TS, TSHist, Limit),
                                hist = Hist2}
                  catch
                      _E:_R ->
                          %io:format("Error ~p: ~p\n", [_E, _R]),
                          %% Any problems with getstat/getopts mark in error
                          erlang:send_after(State#state.clear_after,
                                            self(),
                                            {clear, Socket}),
                          Conn#conn{type = error}
                  end;
             (_Socket, Conn) ->
                  Conn
          end,
    {noreply, State#state{conns = gb_trees:map(Fun, Conns)}};
handle_info({clear, Socket}, State = #state{conns = Conns}) ->
    {noreply, State#state{conns = gb_trees:delete_any(Socket, Conns)}}.

terminate(_Reason, _State) ->
    lager:info("Shutting down TCP Monitor"),
    %% TODO: Consider trying to do something graceful with poolboy?
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Add a distributed connection to the state
add_dist_conn(Node, Port, State) ->
    add_conn(Port, #conn{tag = {node, Node},
                         type = dist,
                         transport = ranch_tcp}, State).

%% Add connection to the state
add_conn(Socket, Conn, State = #state{conns = Conns}) ->
    State#state{conns = gb_trees:enter(Socket, Conn, Conns)}.

%% Update the histogram with the list of name/values
update_hist(Readings, Limit, Histories) ->
    %% For all the readings of {Stat, Val} pairs
    lists:foldl(
      %% Prepend newest reading and truncate
      fun ({Stat, Val}, Histories0) ->
              orddict:update(Stat,
                             fun(Hist) ->
                                     prepend_trunc(Val, Hist, Limit)
                             end,
                             [Val],
                             Histories0)
      end, Histories, Readings).

prepend_trunc(Val, List, Limit) ->
    lists:sublist([Val | List], Limit).

conn_status(Socket, #conn{tag = Tag, type = Type,
                  ts_hist = TsHist, hist = Histories,
                  transport = Transport}, StatusFuns) ->
    Fun = fun({Stat, Hist}, Acc) ->
                         case dict:find(Stat, StatusFuns) of
                             {ok, {Alias, StatusFun}} ->
                                 [{Alias, StatusFun(TsHist, Hist)} | Acc];
                             {ok, StatusFun} ->
                                 [{Stat, StatusFun(TsHist, Hist)} | Acc];
                             _ ->
                                 Acc
                         end
                      end,
    Stats = lists:sort(lists:foldl(Fun, [], Histories)),
    Conn = try % Socket could be dead, don't kill the TCP mon finding out
               Peername = riak_core_util:peername(Socket, Transport),
               Sockname = riak_core_util:sockname(Socket, Transport),
               [{peername, Peername}, {sockname, Sockname}]
           catch
               _:_ ->
                   [{peername, "error"}, {sockname, "error"}]
           end,
    [{tag, Tag}, {type, Type}] ++ Conn ++ Stats.

schedule_tick(State = #state{interval = Interval}) ->
    erlang:send_after(Interval, self(), measurement_tick),
    State.

format_socket_stats([], Buf) -> lists:reverse(Buf);
%format_socket_stats([{K,V}|T], Buf) when K == tag ->
    %format_socket_stats(T, [{tag, V} | Buf]);
format_socket_stats([{K,_V}|T], Buf) when
        K == tag;
        K == sndbuf; 
        K == recbuf;
        K == buffer; 
        K == active;
        K == type;
        K == send_max;
        K == send_avg ->
    %% skip these
    format_socket_stats(T, Buf);
format_socket_stats([{K,V}|T], Buf) when
        K == recv_avg;
        K == recv_cnt;
        K == recv_dvi;
        K == recv_kbps;
        K == recv_max;
        K == send_kbps;
        K == send_pend;
        K == send_cnt ->
    format_socket_stats(T, [{K, lists:flatten(format_list(V))} | Buf]);
format_socket_stats([{K,V}|T], Buf) ->
    format_socket_stats(T, [{K, V} | Buf]).

-ifdef(TEST).
updown() ->
    %% Set the stat gathering interval to 100ms
    {ok, TCPMonPid} = riak_core_tcp_mon:start_link([{interval, 100}]),
    {ok, LS} = gen_tcp:listen(0, [{active, true}, binary]),
    {ok, Port} = inet:port(LS),
    Pid = self(),
    spawn(
        fun () ->
                %% server
                {ok, S} = gen_tcp:accept(LS),
                riak_core_tcp_mon:monitor(S, "test", gen_tcp),
                timer:sleep(1000),
                receive
                    {tcp, S, _Data} ->
                        %% only receive one packet, let the others build
                        %% up
                        ok;
                    _ ->
                        ?assert(fail)
                after
                    1000 ->
                        ?assert(fail)
                end,
                _Stat1 = riak_core_tcp_mon:status(),
                MPid = whereis(riak_core_tcp_mon),
                MPid ! {nodedown, 'foo', []},
                Stat2 = riak_core_tcp_mon:status(),
                MPid ! {nodeup, 'foo', []},
                Stat3 = riak_core_tcp_mon:status(),
                ?assert(proplists:is_defined(socket,hd(Stat2))),
                ?assert(proplists:is_defined(socket,hd(Stat3))),
                gen_tcp:close(S),
                Pid ! finished
        end),
    %% client
    {ok, Socket} = gen_tcp:connect("localhost",Port,
                                   [binary, {active, true}]),
    lists:foreach(
          fun (_) ->
                gen_tcp:send(Socket, "TEST")
          end,
        lists:seq(1,10000)),
    receive
        finished -> ok;
        {'EXIT', _, normal} -> ok;
        X -> io:format(user, "Unexpected message received ~p~n", [X]),
            ?assert(fail)
    end,
    gen_tcp:close(Socket),
    unlink(TCPMonPid),
    exit(TCPMonPid, kill),
    ok.

nodeupdown_test_() ->
    {timeout, 60, fun updown/0}.

-endif.
