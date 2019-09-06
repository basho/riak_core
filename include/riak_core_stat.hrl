-define(CACHE,           app_helper:get_env(riak_core, exometer_cache, {cache, 5000})).
-define(PFX,             riak_core_stat_admin:prefix()).
-define(TIMESTAMP,       riak_core_stat_exometer:timestamp()).

-define(META_ENABLED,    metadata_enabled).

-define(IS_ENABLED(Arg), app_helper:get_env(riak_core, Arg, true)).

-type exometererror() :: no_template | exists | not_found.
-type profileerror()  :: profile_exists_already | no_stats | no_data | no_profile.
-type metaerror()     :: unregistered | no_stat | no_status.
%%-type reason()        :: exometererror() | profileerror() | metaerror() | any().
%%-type error()         :: {error, reason()}.
-type arg()               :: any().

-type value()         :: any().
-type exo_value()     :: {ok, value()}.
-type aliases()       :: list() | atom().
-type info()          :: name | type | module | value | cache| status |
                         timestamp | options | ref | datapoints | entry.
-type datapoint()     :: info() | list() | integer().
-type opt_tup()       :: {atom(), any()}.
-type options()       :: list() | opt_tup().
-type acc()           :: any().

%%-type app()           :: atom().
%%-type statname()      :: atom() | list().
-type type()          :: atom() | tuple().
-type status()        :: enabled | disabled | unregistered.
-type print()         :: any().
-type attr()          :: [info()].
-type stats()         :: list() | tuple().
-type data()          :: any().
-type pfx()           :: riak.
-type incrvalue()     :: non_neg_integer().
-type response()      :: ok | term() | error().

-type pattern()       :: ets:match_spec().
-type timestamp()     :: non_neg_integer().
-type ttl()           :: atom() | integer().

%%%%% PROFILES

-type profilename()   :: [list()] | [binary()] | any().



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(ENVAPP, riak_core).

-define(INSTANCE,              app_helper:get_env(?ENVAPP, instance)).
-define(MONITOR_SERVER,        app_helper:get_env(?ENVAPP, monitor_server)).
-define(MONITOR_LATENCY_PORT,  app_helper:get_env(?ENVAPP, monitor_latency_port)).
-define(MONITOR_STATS_PORT,    app_helper:get_env(?ENVAPP, monitor_stats_port)).

-define(EXCLUDED_DATAPOINTS,   app_helper:get_env(?ENVAPP, endpoint_excluded_datapoints, [ms_since_reset])).
-define(STATS_LISTEN_PORT,     app_helper:get_env(?ENVAPP, stats_listen_port, 9000)).

-define(ENDPOINTTABLE,         endpoint_state).
-define(UDP_KEY,               udp_socket).
-define(WM_KEY,                http_socket).

-define(STATS_UPDATE_INTERVAL, app_helper:get_env(?ENVAPP, endpoint_stats_update_interval, 1000)).
-define(REFRESH_INTERVAL,      app_helper:get_env(?ENVAPP, endpoint_ip_refresh_interval, 30000)).

-define(SPIRAL_TIME_SPAN,      app_helper:get_env(?ENVAPP, endpoint_stats_spiral_time_span, 1000)).
-define(HISTOGRAM_TIME_SPAN,   app_helper:get_env(?ENVAPP, endpoint_stats_histogram_time_span, 1000)).

-define(UDP_OPEN_PORT,         0).
-define(UDP_OPEN_BUFFER,       {buffer, 100*1024*1024}).
-define(UDP_OPEN_SNDBUFF,      {sndbuf,   5*1024*1024}).
-define(UDP_OPEN_ACTIVE,       {active,         false}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type sanitised_data()    :: {{port(), instance(), server_ip()}, stats() | profilename()}.

-type jsonprops()         :: [{atom(), any()}].
-type serviceid()         :: string() | binary().
-type correlationid()     :: string() | binary().

-type socket()            :: inet:socket().
-type server()            :: inet:ip4_address().
-type latency_port()      :: inet:port_number().
-type server_ip()         :: inet:ip4_address().
-type stats_port()        :: inet:port_number().
-type hostname()          :: inet:hostname().
-type instance()          :: string().
