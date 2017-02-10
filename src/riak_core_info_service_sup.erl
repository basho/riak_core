-module(riak_core_info_service_sup).

-behaviour(supervisor).

-export([start_link/0, 
         start_service/4]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

start_service(Registration, Shutdown, Source, Handler) ->
    supervisor:start_child(?SERVER, [Registration, Shutdown, Source, Handler]).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init([]) ->
    ChildSpec = child_spec(),
    SupFlags = {simple_one_for_one, 5, 1000},
    {ok, {SupFlags, [ChildSpec]}}.

child_spec() ->
    {na,
     {riak_core_info_service_process, start_link, []},
     permanent, 2000, worker, [riak_core_info_service_process]}.

