%%%-------------------------------------------------------------------
%%% @doc
%%% Common specification types and definitions used in riak_stat_push
%%% and related modules.
%%% @end
%%%-------------------------------------------------------------------
-include_lib("riak_core/include/riak_stat.hrl").

-type protocol()        :: tcp | udp | '_'.
-type server_ip()       :: inet:ip4_address() | any().
-type push_port()       :: inet:port_number() | port().
-type socket()          :: inet:socket().
-type hostname()        :: inet:hostname().
-type instance()        :: any().

-type sanitised_push()  :: {{push_port(),instance(),server_ip()},metrics()}.
-type listofpush()      :: [pusharg()] | [].

%% information stored in the metadata
-type push_key()        :: {protocol(),instance()}.
-type push_value()      :: {calendar:datetime(),
                            calendar:datetime(),
                            pid(),
                            runnning_tuple(),
                            node(),
                            push_port(),
                            server_ip(),
                            metrics()} | push_map() | atom().
-type push_map()        :: #{original_dt := calendar:datetime(),
                            modified_dt  := calendar:datetime(),
                            pid          := pid(),
                            running      := (true | false),
                            node         := node(),
                            port         := push_port(),
                            server_ip    := server_ip(),
                            stats        := listofstats()}.

-type runnning_tuple()  :: {running, boolean()}.

-type node_or_nodes()   :: node() | [node()].
-type pusharg()         :: {push_key(),push_value()} | list().

-type jsonstats()       :: list().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%% Endpoint Polling Macros %%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(PUSH_PREFIX, {riak_stat_push, ?NODEID}).

-define(REFRESH_INTERVAL,       app_helper:get_env(riak_stat,refresh_interval,     30000)).

-define(SPIRAL_TIME_SPAN,       app_helper:get_env(riak_stat,spiral_time_span,      1000)).
-define(HISTOGRAM_TIME_SPAN,    app_helper:get_env(riak_stat,historgram_time_span,  1000)).
-define(STATS_LISTEN_PORT,      app_helper:get_env(riak_stat,stat_listen_port,      9000)).
-define(STATS_UPDATE_INTERVAL,  app_helper:get_env(riak_stat,stat_update_interval,  1000)).

-define(BUFFER,                 {buffer,    100*1024*1024}).
-define(SNDBUF,                 {sndbuf,      5*1024*1024}).
-define(ACTIVE,                 {active,    true}).
-define(REUSE,                  {reuseaddr, true}).

-define(OPTIONS,                [?BUFFER, ?SNDBUF, ?ACTIVE, ?REUSE]).

