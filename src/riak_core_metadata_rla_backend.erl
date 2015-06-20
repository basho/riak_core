%% -------------------------------------------------------------------
%%
%% riak_core_metadata_rla_backend.erl: A cluster metadata storage backend
%%                                     for RLA record database
%%
%% Copyright (c) 2015 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc A storage backend for RLA record database utilizing Riak
%%      cluster metadata.  This is appropriate for cluster-local RLA
%%      instances serving the intra-cluster clients.

-module(riak_core_metadata_rla_backend).

-behaviour(riak_net_rla_db).

-export([init/1, close/0, purge/0,
         get/1, put/2, delete/1, list/1]).

-define(RLA_METADATA_PREFIX, {rla, records}).


-spec init(proplists:proplist()) -> ok.
init(_Options) ->
    ok.


-spec close() -> ok.
close() ->
    ok.


-spec purge() -> ok.
purge() ->
    purge_(riak_core_metadata:iterator(?RLA_METADATA_PREFIX)).

-spec purge_(riak_core_metadata:iterator()) -> ok.
purge_(It) ->
    case riak_core_metadata:itr_done(It) of
        true ->
            riak_core_metadata:itr_close(It),
            ok;
        false ->
            riak_core_metadata:delete(
              ?RLA_METADATA_PREFIX,
              riak_core_metadata:itr_key(It)),
            purge_(
              riak_core_metadata:itr_next(It))
    end.


-spec get(riak_net_rla:url_as_key()) ->
                 {ok, [riak_net_rla:ep()]} | {error, not_found} | {error, term()}.
get(Url) ->
    case riak_core_metadata:get(?RLA_METADATA_PREFIX, Url) of
        undefined ->
            {error, not_found};
        %% key is not stripped from the tuple being returned, as it is
        %% there as returned from ets:lookup
        {EPList, _Timestamp} when is_list(EPList) ->
            {ok, EPList};
        {error, Reason} ->
            {error, Reason}
    end.


-spec put(riak_net_rla:url_as_key(), [riak_net_rla:ep()]) -> ok.
put(Url, EPList) ->
    %% this is necessary to overwrite the old value with a new timestamp
    case riak_core_metadata:get(?RLA_METADATA_PREFIX, Url) of
        undefined ->
            fine;
        _ ->
            riak_core_metadata:delete(?RLA_METADATA_PREFIX, Url)
    end,
    ok = riak_core_metadata:put(
           ?RLA_METADATA_PREFIX, Url, {EPList, os:timestamp()}).


-spec delete(riak_net_rla:url_as_key()) -> ok.
delete(Url) ->
    ok = riak_core_metadata:delete(?RLA_METADATA_PREFIX, Url).
%% Because riak_core takes care of propagating deletions across all
%% metadata holding nodes, we don't have to contrive our own
%% tombstomes (nor do we need to sync between RLA instnces, and
%% neither is there a case for running multiple instances of RLA in a
%% single cluster).


-spec list(riak_net_rla:url_as_key()) ->
                  {ok, [riak_net_rla:path_element()]} | {error, term()}.
list(UrlPrefix) ->
    UrlPrefixLen = length(UrlPrefix),
    Children =
        riak_core_metadata:fold(
          fun({Url, NotDeleted}, Acc)
                when length(Url) > UrlPrefixLen,
                     NotDeleted /= ['$deleted'] ->
                  case prefix_match(Url, UrlPrefix) of
                      true ->
                          [lists:nth(UrlPrefixLen + 1, Url) | Acc];
                      false ->
                          Acc
                  end;
             (_, Acc) ->
                  Acc
          end,
          [], ?RLA_METADATA_PREFIX, []),
          %% %% riak_core_metadata iterators support native ets matching
          %% %% expressions, whee
          %% [{{UrlPrefix ++ '$1', '_', '_'},
          %%   [{'>', {size, '$1'}, 0}],
          %%   %% will this work? filtering on the returned object?
          %%   %% [{hd, '$1'}]}])}. % nope
          %%   []}]),
    {ok, lists:usort(Children)}.

prefix_match(_, []) ->
    true;
prefix_match([], _) ->
    true;
prefix_match([H|T1], [H|T2]) ->
    prefix_match(T1, T2);
prefix_match(_, _) ->
    false.
