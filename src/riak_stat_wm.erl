%%%-------------------------------------------------------------------
%%% @doc
%%% A mimic of riak_kv_wm_stats,
%%% @end
%%%-------------------------------------------------------------------
-module(riak_stat_wm).
%%-include_lib("webmachine/include/webmachine.hrl").

%% wm resource exports
-export([
    init/1,
    encodings_provided/2,
    content_types_provided/2,
    service_available/2,
    forbidden/2,
    produce_body/2,
    pretty_print/2]).
-export([
    get_stats/0
]).

-record(ctx, {
    prefix,     %% prefix for resource uris
    riak        %% local | {node(), atom()}
}).

-type props()       :: proplists:proplist().
-type context()     :: #ctx{}.
-type rd ()         :: webmachine:wrq().
-type encoding()    :: string().
-type encoder()     :: fun((iodata()) -> iodata()).
-type enc_prov()    :: [{encoding(), encoder()}].
-type media()       :: string().
-type handler()     :: atom().
-type ctype()       :: [{media(), handler()}].
-type halt()        :: {error, term()} | {halt, 200..599}.

-define(CONTENT_TYPE, <<"content-type">>).
-define(PLAIN_TEXT_CONTENT_TYPE,    "text/plain").
-define(JSON_CONTENT_TYPE,    "application/json").
-define(PLAIN_TEXT_HEADER, [{?CONTENT_TYPE, ?PLAIN_TEXT_CONTENT_TYPE}]).
-define(JSON_HEADER,             [{?CONTENT_TYPE, ?JSON_CONTENT_TYPE}]).
-define(DEFAULT_ENCODINGS, riak_kv_wm_utils:default_encodings()).

%%--------------------------------------------------------------------

%%%-------------------------------------------------------------------
%% @doc
%% receives a config property list from dispatcher as the Arg, for
%% every request to this module, if successful should return {ok, Context},
%% Initialise the resource and pull out needed properties
%% @end
%%%-------------------------------------------------------------------
-spec(init(props()) -> {ok, context()}).
init(Props) ->
    {ok, #ctx{prefix = proplists:get_value(prefix, Props),
        riak   = proplists:get_value(riak,   Props)
    }}.


%%%-------------------------------------------------------------------
%% @doc
%% Get a list of encodings that this resource provides,
%% "identity" -> all methods
%% "gzip"     -> GET method
%% @end
%%%-------------------------------------------------------------------
-spec(encodings_provided(rd(), context()) -> {enc_prov(), rd(), context()}).
encodings_provided(Req, Ctx) ->
    case wrq:method(Req) of
        'GET' ->
            {?DEFAULT_ENCODINGS, Req, Ctx};
        _ ->
            {[{"identity", fun(X) -> X end}], Req, Ctx}
    end.


%%%-------------------------------------------------------------------
%% @doc
%% Return the list of pairs -> the media in ctype() being a string
%% of content-type format (such as jsonheader) and the handler being
%% and atom of the function that can provide a representation of the media
%% @end
%%%-------------------------------------------------------------------
-spec(content_types_provided(rd(), context()) ->
                                          {ctype(), rd(), context()}).
content_types_provided(Req, Ctx) ->
    {[{?JSON_CONTENT_TYPE, produce_body},
        {?PLAIN_TEXT_CONTENT_TYPE, pretty_print}],
        Req, Ctx}.

%%%-------------------------------------------------------------------
%% @doc
%% Determine a connection to riak
%% @end
%%%-------------------------------------------------------------------
-spec(service_available(rd(), context()) ->
                               {boolean() | halt(), rd(), context()}).
service_available(Req, Ctx=#ctx{}) ->
    {true, Req, Ctx}.


%%%-------------------------------------------------------------------
%% @doc
%% determines whether the request is forbidden
%% @end
%%%-------------------------------------------------------------------
-spec(forbidden(rd(), context()) ->
                               {boolean() | halt(), rd(), context()}).
forbidden(Req, Ctx) ->
    {riak_kv_wm_utils:is_forbidden(Req), Req, Ctx}.


%%%-------------------------------------------------------------------
%% @doc
%% Retrieve the stats and return an encoded json object to send back
%% @end
%%%-------------------------------------------------------------------
-spec(produce_body(rd(), context()) -> {iodata(), rd(), context()}).
produce_body(ReqData, Ctx) ->
    Stats = get_stats(),
    Body  = riak_stat_json:encode({struct, Stats}),
    {Body, ReqData, Ctx}.


%%%-------------------------------------------------------------------
%% @doc
%% Format the response JSON object in a "pretty-printed" style.
%% @end
%%%-------------------------------------------------------------------
-spec(pretty_print(rd(), context()) -> {string(), rd(), context()}).
pretty_print(RD1, C1=#ctx{}) ->
    {Json, RD2, C2} = produce_body(RD1, C1),
    {json_pp:print(binary_to_list(list_to_binary(Json))), RD2, C2}.


%%--------------------------------------------------------------------

get_stats() ->
    Stats = riak_stat_exom:get_values(['_']),
    lists:map(fun({N,V}) ->
        NewName = parse_name(N),
        NewValues = parse_values(V),
        {NewName,NewValues}
              end, Stats).


parse_name([riak | Name]) ->
    parse_name(Name);
parse_name(Name) ->
    [App | Stat] = Name,
    S = atom_to_list(App),
    T = ": ",
    R = stringifier(Stat),
    S++T++R.

stringifier(Stat) ->
    lists:foldr(fun
                    (S,Acc) when is_list(S) ->
                        S++" "++Acc;
                    (I,Acc) when is_integer(I) ->
                        integer_to_list(I)++" "++Acc;
                    ({IpAddr,Port},Acc) when is_tuple(IpAddr)->
                        T = ip_maker(IpAddr),
                        P = integer_to_list(Port),
                        "{"++T++","++P++"}"++" "++Acc;
                    ({IpAddr,Port},Acc) when is_list(IpAddr)->
                        T = IpAddr,
                        P = integer_to_list(Port),
                        "{"++T++","++P++"}"++" "++Acc;
                    (T,Acc) when is_tuple(T) ->
                        tuple_to_list(T)++" "++Acc;
                    (L,Acc) when is_atom(L) ->
                        atom_to_list(L)++" "++Acc;
                    (_Other,Acc) ->
                        Acc
                end, " = ",Stat).

ip_maker(IpAddr) ->
    Ip = tuple_to_list(IpAddr),
    NewIp = [integer_to_list(N) || N <- Ip],
    lists:join(".",NewIp).

parse_values(Values) ->
    lists:foldl(fun
                    ({ms_since_reset,_V},Acc) -> Acc;
                    (V,Acc) -> [V|Acc]
                end, [],Values).
