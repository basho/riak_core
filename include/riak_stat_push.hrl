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
                            metrics()}.

-type runnning_tuple()  :: {running, boolean()}.

-type node_or_nodes()   :: node() | [node()].
-type pusharg()         :: {push_key(),push_value()}.

-type jsonstats()       :: list().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%% Endpoint Polling Macros %%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(MONITORSTATSPORT,       app_helper:get_env(riak_stat,monitor_stats_port,8001)).
-define(MONITORSERVER,          app_helper:get_env(riak_stat,monitor_server,"127.0.0.1")).

-define(REFRESH_INTERVAL,       30000).

-define(SPIRAL_TIME_SPAN,       1000).
-define(HISTOGRAM_TIME_SPAN,    1000).
-define(WM_KEY,                 http_socket).
-define(STATS_LISTEN_PORT,      9000).
-define(STATS_UPDATE_INTERVAL,  1000).
