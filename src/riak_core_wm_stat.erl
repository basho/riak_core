%% -------------------------------------------------------------------
%%
%% stats_http_resource: publishing Riak runtime stats via HTTP
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(riak_core_wm_stat).

%% webmachine resource exports
-export([
         init/1,
         content_types_provided/2,
         service_available/2,
         resource_exists/2,
         forbidden/2,
         produce_body/2,
         pretty_print/2
        ]).

-include_lib("webmachine/include/webmachine.hrl").

-define(WILCARD, '_').

-record(ctx, {names_and_types}).

init(_) ->
    {ok, #ctx{}}.

%% @spec content_types_provided(webmachine:wrq(), context()) ->
%%          {[ctype()], webmachine:wrq(), context()}
%% @doc Get the list of content types this resource provides.
%%      "application/json" and "text/plain" are both provided
%%      for all requests.  "text/plain" is a "pretty-printed"
%%      version of the "application/json" content.
content_types_provided(ReqData, Context) ->
    {[{"application/json", produce_body},
      {"text/plain", pretty_print}],
     ReqData, Context}.

service_available(ReqData, Ctx) ->
    {true, ReqData, Ctx}.

resource_exists(RD, Ctx) ->
    case parse_path(wrq:path_tokens(RD), []) of
        {error, non_existant_atom, _PathElement} ->
            {false, RD, Ctx};
        Path ->
            case riak_core_stat_q:names_and_types(Path) of
                [] ->
                    {false, RD, Ctx};
                NamesAndType ->
                    {true, RD, Ctx#ctx{names_and_types=NamesAndType}}
            end
    end.

parse_path([], Atoms) ->
    lists:reverse(Atoms);
parse_path([Elem|Path], Atoms) ->
    case (catch list_to_existing_atom(Elem)) of
        {'EXIT', {badarg, _}} ->
            {error, non_existant_atom, Elem};
        Atom ->
            parse_path(Path, [Atom|Atoms])
    end.

forbidden(RD, Ctx) ->
    {riak_kv_wm_utils:is_forbidden(RD), RD, Ctx}.

produce_body(ReqData, Ctx=#ctx{names_and_types=NamesAndType}) ->
    Body = mochijson2:encode({struct, get_stats(NamesAndType)}),
    {Body, ReqData, Ctx}.

%% @spec pretty_print(webmachine:wrq(), context()) ->
%%          {string(), webmachine:wrq(), context()}
%% @doc Format the respons JSON object is a "pretty-printed" style.
pretty_print(RD1, C1=#ctx{}) ->
    {Json, RD2, C2} = produce_body(RD1, C1),
    {json_pp:print(binary_to_list(list_to_binary(Json))), RD2, C2}.

get_stats(NamesAndType) ->
    Stats = riak_core_stat_q:calculate_stats(NamesAndType),
    lists:flatten([flatten_stat(Name, Value) || {Name, Value} <- Stats]).

flatten_stat(Name, Value) ->
    FlatName = join(tuple_to_list(Name), <<>>),
    case is_list(Value) of
        true ->            [flatten_stat(FlatName, ValName, Val) || {ValName, Val} <- Value];
        false -> {FlatName, Value}
    end.

flatten_stat(StatName, PartName, PartVal) ->
    {join([StatName, PartName], <<>>), PartVal}.

join([], Bin) ->
    binary_to_atom(Bin, latin1);
join([Atom|Rest], <<>>) ->
    Bin2 = atom_to_binary(Atom, latin1),
    join(Rest, <<Bin2/binary>>);
join([Atom|Rest], Bin) ->
    Bin2 = atom_to_binary(Atom, latin1),
    join(Rest, <<Bin/binary, $_, Bin2/binary>>).
