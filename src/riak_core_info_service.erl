%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Basho Technologies, Inc.  All Rights Reserved.
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

%%% @doc The riak_core_info_service is a way for dependencies of riak_core to sign
%%%      themselves up to receive messages from riak_core without having to generate
%%%      a cyclic call graph.
%%%      The dependency needs to have a way to be told where it should send requests
%%%      to get information out of riak_core. That is the Registration callback().
%%%      The Source callback() is a function inside riak_core that returns the
%%%      information that the dependency needs.
%%%      The Handler callback() is a function that will process the messages sent out
%%%      of the dependency when it wants information.
%%%      The dependency MUST send messages to the info service pid of the form
%%%        {invoke, SourceParameters::[term()], HandlerContext::[term()]}
%%%
%%%      The information service originates from a need in eleveldb to know something
%%%      about bucket properties.
%%%      For that particular problem the callback()s would look like this:
%%%      Registration = {eleveldb, set_metadata_pid, []}
%%%      Source = {riak_core_bucket, get_bucket, []}
%%%      Handler = {eleveldb, handle_metadata_response, []}
%%%      And the handle_metadata_response function would look like this:
%%%      handle_metadata_response({Props, _SourceParams, [Key]}) ->
%%%          property_cache(Key, Props).
%%%
%%%      set_metadata_pid(_Pid) ->
%%%          erlang:nif_error({error, not_loaded}).
%%%


-module(riak_core_info_service).

-export([start_service/4]).


-type callback() :: {module(), FunName::atom(),InitialArgs::[term()]} | undefined.

-export_type([callback/0]).

-spec start_service(Registration::callback(), Shutdown::callback(), Source::callback(), Handler::callback()) ->
                           ok |
                           {error, term()}.

start_service(Registration, Shutdown, Source, Handler) ->
    riak_core_info_service_sup:start_service(Registration, Shutdown, Source, Handler).
