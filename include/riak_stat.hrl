%%%-------------------------------------------------------------------
%%% @doc
%%% Common specification types and definitions used in riak_stat
%%% @end
%%%-------------------------------------------------------------------

%% Stat Types

-type app()         :: atom().
-type statslist()   :: [metricname()].
-type metricname()  :: [atom()] | atom().
-type statname()    :: metricname().

-type statinfo()    :: {metricname(),type(),options(),aliases()}.
-type status()      :: enabled | disabled | unregistered | '_'.
-type type()        :: atom() | any().
-type options()     :: list() | [] | opt_tup().
-type aliases()     :: list() | [].
-type datapoint()   :: info() | list() | integer().
-type data()        :: any().
-type sanitised()   :: {metricname(),status(),type(),datapoint()}.
-type opt_tup()       :: {atom(), any()}.


-type arg()         :: any().
-type arguments()   :: [] | arg() |
                           {arg(),arg()}|
                           {arg(),arg(),arg()}|
                           {arg(),arg(),arg(),arg()}.

-type aliastype()   :: new | prefix_foldl | regexp_foldr.
-type value()         :: any().
-type exo_value()     :: {ok, value()}.
-type info()          :: name | type | module | value | cache| status |
                         timestamp | options | ref | datapoints | entry.
-type acc()           :: any().

-type profilename()   :: [list()] | [binary()] | any().

-type exometererror() :: no_template | exists | not_found.
-type profileerror()  :: profile_exists_already | no_stats | no_data | no_profile.
-type metaerror()     :: unregistered | no_stat | no_status.
-type error()       :: {error, reason()}.
-type reason()      :: any() | exometererror() | profileerror() | metaerror().

-type pattern()     :: ets:match_spec().
-type timestamp()   :: non_neg_integer().
-type ttl()         :: atom() | integer().

-type print()         :: any().
-type attr()          :: [info()].
-type stats()         :: list() | tuple().

-type incrvalue()     :: non_neg_integer().
-type response()      :: ok | term() | error().

-type socket()            :: inet:socket().
-type server()            :: inet:ip4_address().
-type latency_port()      :: inet:port_number().
-type server_ip()         :: inet:ip4_address().
-type stats_port()        :: inet:port_number().
-type hostname()          :: inet:hostname().
-type instance()          :: string().
-type jsonprops()         :: [{atom(), any()}].
-type serviceid()         :: string() | binary().
-type correlationid()     :: string() | binary().

%% Stat Macros

-define(IS_ENABLED(Arg),    app_helper:get_env(riak_core,Arg,true)).
-define(METADATA_ENABLED,   metadata_enabled).

-define(PFX,             riak_stat:prefix()).


%% Endpoint Polling Macros

-define(INSTANCE,              "riak_stat_latency-polling").
-define(MONITOR_SERVER,        "127.0.0.1").
-define(MONITOR_LATENCY_PORT,  10075).
-define(MONITOR_STATS_PORT,    10085).
-define(ENDPOINTTABLE,         endpoint_state).

