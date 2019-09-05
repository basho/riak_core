%%%-------------------------------------------------------------------
%%% @doc
%%% Common specification types and definitions used in riak_stat
%%% @end
%%%-------------------------------------------------------------------

%% Stat Types

-type app()         :: atom().
-type statslist()   :: [metricname()].
-type metricname()  :: [atom()] | atom().

-type statinfo()    :: {metricname(),type(),options(),aliases()}.
-type status()      :: enabled | disabled | unregistered.
-type type()        :: atom() | any().
-type options()     :: list() | [].
-type aliases()     :: list() | [].
-type datapoint()   :: info() | list() | integer().


-type arg()         :: any().
-type arguments()   :: [] | arg() |
                           {arg(),arg()}|
                           {arg(),arg(),arg()}|
                           {arg(),arg(),arg(),arg()}.
-type function()    :: any().

-type aliastype()   :: new | prefix_foldl | regexp_foldr.

-type error()       :: {error, reason()}.
-type reason()      :: any().

-type pattern()     :: ets:match_spec().
-type timestamp()   :: non_neg_integer().
-type ttl()         :: atom() | integer().

%% Stat Macros

