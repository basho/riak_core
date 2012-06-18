%%%-------------------------------------------------------------------
%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2012, Russell Brown
%%% @doc
%%%
%%% @end
%%% Created : 16 Jun 2012 by Russell Brown <russelldb@basho.com>
%%%-------------------------------------------------------------------
-module(riak_core_stat_cache).

-behaviour(gen_server).

%% API
-export([start_link/0, get_stats/1, register_app/2, register_app/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(TTL, 5).

-record(state, {tab, active=orddict:new(), apps=orddict:new()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

register_app(App, {M, F, A}) ->
    TTL = app_helper:get_env(riak_core, stat_cache_ttl, ?TTL),
    register_app(App, {M, F, A}, TTL).

register_app(App, {M, F, A}, TTL) ->
    gen_server:call(?SERVER, {register, App, {M, F, A}, TTL}).

get_stats(App) ->
    gen_server:call(?SERVER, {get_stats, App}).

init([]) ->
    Tab = ets:new(?MODULE, [public, set, named_table]),
    {ok, #state{tab=Tab}}.

handle_call({register, App, {Mod, Fun, Args}, TTL}, _From, State0=#state{apps=Apps0}) ->
    Apps = case registered(App, Apps0) of
               false ->
                   folsom_metrics:new_histogram({?MODULE, Mod}),
                   folsom_metrics:new_meter({?MODULE, App}),
                   orddict:store(App, {Mod, Fun, Args, TTL}, Apps0);
               {true, _} ->
                   Apps0
           end,
    {reply, ok, State0#state{apps=Apps}};
handle_call({get_stats, App}, From, State0=#state{apps=Apps, active=Active0, tab=Tab}) ->
    Reply = case registered(App, Apps) of
                false ->
                    {reply, {enotregistered, App}, State0};
                {true, {M, F, A, TTL}} ->
                    case cache_get(App, Tab, TTL) of
                        No when No == miss; No == stale ->
                            Active = maybe_get_stats(App, From, Active0, {M, F, A}),
                            {noreply, State0#state{active=Active}};
                        Hit ->
                            {reply, Hit, State0}
                    end
            end,
    Reply;
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({stats, App, Stats}, State0=#state{tab=Tab, active=Active}) ->
    ets:insert(Tab, {App, folsom_utils:now_epoch(), Stats}),
    State = case orddict:find(App, Active) of
                {ok, Awaiting} ->
                    [gen_server:reply(From, Stats) || From <- Awaiting],
                    State0#state{active=orddict:erase(App, Active)};
                error ->
                    State0
            end,
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% internal
registered(App, Apps) ->
    registered(orddict:find(App, Apps)).

registered(error) ->
    false;
registered({ok, Val}) ->
    {true, Val}.

cache_get(App, Tab, TTL) ->
    Res = case ets:lookup(Tab, App) of
              [] ->
                  miss;
              [Hit] ->
                  check_freshness(Hit, TTL)
          end,
    Res.

check_freshness({_App, TStamp, Stats}, TTL) ->
    case (TStamp + TTL) > folsom_utils:now_epoch() of
        true ->
            Stats;
        false ->
            stale
    end.

maybe_get_stats(App, From, Active, {M, F, A}) ->
    %% if a get stats is not under way start one
    Awaiting = case orddict:find(App, Active) of
                   error ->
                       do_get_stats(App, {M, F, A}),
                       [From];
                   {ok, Froms} ->
                       [From|Froms]
               end,
    orddict:store(App, Awaiting, Active).

do_get_stats(App, {M, F, A}) ->
    spawn_link(fun() ->
                       Stats = folsom_metrics:histogram_timed_update({?MODULE, M}, M, F, A),
                       folsom_metrics:notify_existing_metric({?MODULE, App}, 1, meter),
                       gen_server:cast(?MODULE, {stats, App, Stats}) end).
