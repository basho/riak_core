%%%-------------------------------------------------------------------
%%% @doc
%%% Common specification types and definitions used in riak_stat_push
%%% and related modules.
%%% @end
%%%-------------------------------------------------------------------
-include_lib("riak_core/include/riak_stat.hrl").

-type protocol()        :: tcp | udp | http.
-type server_ip()       :: inet:ip4_address().
-type push_port()       :: inet:port_number() | port().
-type socket()          :: inet:socket().
-type hostname()        :: inet:hostname().
-type instance()        :: string() | list().

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
                            metrics()} | push_map().
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
-type pusharg()         :: {push_key(),push_value()}.

-type jsonstats()       :: list().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%% Endpoint Polling Macros %%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(PUSHPREFIX, {riak_stat_push, ?NODEID}).

-define(DEFAULTPORT,            app_helper:get_env(riak_stat,default_port,8005)).
-define(DEFAULTSERVER,          app_helper:get_env(riak_stat,default_server,"127.0.0.1")).

-define(MONITORSTATSPORT,       app_helper:get_env(riak_stat,monitor_stats_port,?DEFAULTPORT)).
-define(MONITORSERVER,          app_helper:get_env(riak_stat,monitor_server,?DEFAULTSERVER)).

-define(REFRESH_INTERVAL,       app_helper:get_env(riak_stat,refresh_interval,30000)).

-define(SPIRAL_TIME_SPAN,       app_helper:get_env(riak_stat,spiral_time_span,1000)).
-define(HISTOGRAM_TIME_SPAN,    app_helper:get_env(riak_stat,historgram_time_span,1000)).
-define(STATS_LISTEN_PORT,      app_helper:get_env(riak_stat,stat_listen_port,9000)).
-define(STATS_UPDATE_INTERVAL,  app_helper:get_env(riak_stat,stat_update_interval,1000)).

-define(BUFFER,                 app_helper:get_env(riak_stat,buffer, 100*1024*1024)).
-define(SNDBUF,                 app_helper:get_env(riak_stat,sndbuf,   5*1024*1024)).
-define(ACTIVE,                 app_helper:get_env(riak_stat,active,          true)).
-define(REUSE,                  app_helper:get_env(riak_stat,reuseaddr,       true)).

-define(OPTIONS,   [{buffer,    ?BUFFER},
                    {sndbuf,    ?SNDBUF},
                    {active,    ?ACTIVE},
                    {reuseaddr, ?REUSE}]).

-define(ATTEMPTS,               app_helper:get_env(riak_stat_push, restart_attempts, 50)).

