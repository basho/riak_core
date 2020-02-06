%%%-------------------------------------------------------------------
%%% @doc
%%% Common specification types and definitions used in riak_stat
%%% @end
%%%-------------------------------------------------------------------
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%% Stat Specification Types %%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%% Arguments
% StatName :: [prefix(),app()|[statname()]]
-type metrics()         :: [metricname()].
-type metricname()      :: [prefix() | [app() | [statname()]]] | [statname()]
                            | [tuple_stat()] | atom().
-type prefix()          :: atom(). %% 'riak'
-type app()             :: atom(). %% i.e. 'riak_core'
-type statname()        :: [atom()] | atom() | mfa(). %% [riak,stat|'_']

-type listofstats()     :: [metricname()] | [atom()].

-type datapoints()      :: [datapoint()] | [].
-type datapoint()       :: mean | max | min | mean | mode | 99 | 95 | 90
                         | 100 | 75 | 50 | value | default | other().

-type consolearg()      :: [string()] | [atom()] | list() | [].
-type profilename()     :: list() | [string()].
-type profile_prefix()  :: {atom(), atom()}.


% StatInfo :: {Statname, Type, Status, Aliases}
-type tuple_stat()      :: {metricname(),type(),options(),aliases()}
                            | {atom(),atom()}.
-type type()            :: exometer:type().
-type options()         :: [exometer:options()|[statusopts()|[cacheopts()]]]
                            | [] | list() | any().
-type aliases()         :: exometer_alias:alias().

-type statusopts()      :: [{status,status()}].
-type status()          :: enabled | disabled | unregistered | '_'.

-type cacheopts()       :: [{cache,cache()}].
-type cache()           :: non_neg_integer().

% function specific
-type pattern()         :: ets:match_spec().
-type incrvalue()       :: non_neg_integer() | integer() | float().


%%% Return arguments

% VALUES
-type nts_stats()       :: [{metricname(),type(),status()}]. %% nts = {Name, Type, Status}
-type n_v_stats()       :: [{metricname(),stat_value()}].    %% n_v = {Name, Value/Values}
-type n_i_stats()       :: [{metricname(),stat_info()}].     %% n_i = {Name,  Information}
-type n_s_stats()       :: [{metricname(),(status() |statusopts())}].

-type stat_value()      :: exo_value() | values().
-type exo_value()       :: {ok, values()}.
-type values()          :: [value()] | [] | value().
-type value()           :: integer() | list() | atom() | binary().

-type stat_info()       :: [{info(),values()}] | [].        %% [{value,0}...]
-type info()            :: name | type | module | value | cache| status
                         | timestamp | options | ref | datapoints | entry.
-type attributes()      :: [info()] | [].
-type sanitised_stat()  :: {metricname(),status(),type(),datapoints()}.

-type print()           :: list() | string() | [] | ok.
-type error()           :: {error, reason()} | error.
-type reason()          :: generalerror() | exometererror() | profileerror() | metaerror().
-type exometererror()   :: no_template | exists | not_found.
-type profileerror()    :: profile_exists_already | no_stats | no_data | no_profile.
-type metaerror()       :: unregistered | no_stat | no_status.
-type generalerror()    :: badarg | econnrefused | other().
-type other()           :: any().
-type acc()             :: any().

-type timestamp()       :: erlang:timestamp().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%% Stat Macros %%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(PREFIX,             riak).
-define(NODEID,             node()).

-define(METADATA_ENV, metadata_enabled).

-define(INFOSTAT,[name,type,module,value,cache,status,timestamp,options]).
%%                  attributes for all the metrics stored in exometer