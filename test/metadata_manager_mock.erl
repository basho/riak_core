%%% File        : metadata_manager_mock.erl
%%% Author      : Ulf Norell
%%% Description : 
%%% Created     : 20 Aug 2013 by Ulf Norell
-module(metadata_manager_mock).

-compile(export_all).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-behaviour(riak_core_broadcast_handler).

-export([broadcast_data/1,
         merge/2,
         is_stale/1,
         graft/1]).

-record(state, {time, dict = []}).

-define(SERVER, ?MODULE).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

broadcast_data({Key, _Val}) ->
  gen_server:call(?SERVER, {get_data, Key}).

put(Key, Val) ->
  gen_server:call(?SERVER, {put, Key, Val}).

merge(Key, Body) ->
  gen_server:call(?SERVER, {merge, Key, Body}).

is_stale(Key) ->
  gen_server:call(?SERVER, {is_stale, Key}).

graft(Key) ->
  gen_server:call(?SERVER, {graft, Key}).

stop() ->
  gen_server:call(?SERVER, stop).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([]) ->
  Time = dvvset:join(dvvset:update(dvvset:new(dummy), node_name())),
  {ok, #state{ time = Time }}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) ->
%%                {reply, Reply, State} |
%%                {reply, Reply, State, Timeout} |
%%                {noreply, State} |
%%                {noreply, State, Timeout} |
%%                {stop, Reason, Reply, State} |
%%                {stop, Reason, State}
%% Description: Handling call dict
%%--------------------------------------------------------------------
handle_call({get_data, Key}, _From, State) ->
  {Key, VSet} = lists:keyfind(Key, 1, State#state.dict),
  {reply, {{Key, dvvset:join(VSet)}, VSet}, State};
handle_call({put, Key, Val}, _From, State) ->
  case lists:keyfind(Key, 1, State#state.dict) of
    false ->
      VSet    = dvvset:update(dvvset:new(State#state.time, Val), node_name()),
      NewDict = [{Key, VSet} | State#state.dict],
      event_logger:event({put, node_name(), Key, VSet, new}),
      {reply, VSet, State#state{ dict = NewDict, time = dvvset:join(VSet) }};
    {Key, VSet} ->
      VSet1   = dvvset:update(dvvset:new(State#state.time, Val), VSet, node_name()),
      NewDict = lists:keystore(Key, 1, State#state.dict, {Key, VSet1}),
      event_logger:event({put, node_name(), Key, Val, update, VSet1}),
      {reply, VSet1, State#state{ dict = NewDict, time = dvvset:join(VSet1) }}
  end;
handle_call({merge, {Key, _Time}, New}, _From, State) ->
  case lists:keyfind(Key, 1, State#state.dict) of
    false ->
      NewDict = [{Key, New} | State#state.dict],
      %% TODO: right time?
      NewTime = dvvset:join(dvvset:sync([dvvset:new(State#state.time, dummy), New])),
      event_logger:event({merge, node_name(), Key, New, new}),
      {reply, true, State#state{ dict = NewDict, time = NewTime }};
    {Key, Old} ->
      case less_or_equal(New, Old) of
        true ->
          event_logger:event({merge, node_name(), Key, New, Old, stale}),
          {reply, false, State};
        false ->
          VSet2   = dvvset:sync([Old, New]),
          NewDict = lists:keystore(Key, 1, State#state.dict, {Key, VSet2}),
          %% TODO: right time?
          NewTime = dvvset:join(dvvset:sync([dvvset:new(State#state.time, dummy), VSet2])),
          event_logger:event({merge, node_name(), Key, New, merge}),
          {reply, true, State#state{ dict = NewDict, time = NewTime }}
      end
  end;
handle_call({is_stale, {Key, Time}}, _From, State) ->
  Stale =
    case lists:keyfind(Key, 1, State#state.dict) of
      false     ->
        event_logger:event({is_stale, node_name(), Key, Time, false}),
        false;
      {_, VSet} ->
        St = less_or_equal(dvvset:new(Time, dummy), VSet),
        event_logger:event({is_stale, node_name(), Key, Time, St}),
        St
    end,
  {reply, Stale, State};
handle_call({graft, {Key, Time}}, _From, State) ->
  case lists:keyfind(Key, 1, State#state.dict) of
    {Key, VSet} ->
      case {equal(Time, VSet), less(dvvset:new(Time, dummy), VSet)} of
        {false, false} ->
          event_logger:event({graft, node_name(), Key, bad_graft, Time, VSet}),
          {reply, stale, State};
        _ ->
          event_logger:event({graft, node_name(), Key, VSet}),
          {reply, {ok, VSet}, State}
      end;
    false ->
      event_logger:event({graft, node_name(), Key, bad_graft, Time}),
      {reply, stale, State}
  end;
handle_call(stop, _From, State) ->
  Dict = State#state.dict,
  {stop, normal, lists:sort(Dict), State};
handle_call(Other, _From, State) ->
  io:format("BAD CALL: ~p\n~p\n", [Other, State]),
  {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast dict
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast dict
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

node_name() ->
  list_to_atom(hd(string:tokens(atom_to_list(node()),"@"))).

less_or_equal(A, B) -> less(A, B) orelse equal(A, B).

equal(A, B) when is_tuple(A) -> equal(dvvset:join(A), B);
equal(A, B) when is_tuple(B) -> equal(A, dvvset:join(B));
equal(A, B) when is_list(A), is_list(B) ->
  lists:sort(A) == lists:sort(B).

less(A, B) -> dvvset:less(A, B).

