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

%%% @doc
%%%      The `riak_core_info_service' is a way for dependencies of
%%%      `riak_core' to be registered to receive messages from
%%%      `riak_core' without a cyclic dependency graph.
%%%
%%%      == Callbacks ==
%%%      The dependency needs to know to which pid it should send
%%%      requests for `riak_core' data. The pid will be sent to the
%%%      `Registration' callback.
%%%
%%%      The `Provider' callback is a function inside `riak_core' that returns
%%%      information that the dependency needs.
%%%
%%%      The `Handler' callback is a function in the dependency that
%%%      expects the reply from the `Provider'.
%%%
%%%      === Handler Parameters ===
%%%      The arguments to `Handler' will be, in order:
%%%      <ol><li>Each item, if any, provided in the list in the 3rd element of the registration tuple</li>
%%%      <li>The result from `Provider' wrapped in a 3-tuple:
%%%        <ol><li>The result</li>
%%%            <li>The list of parameters passed to `Provider'</li>
%%%            <li>The opaque `HandlerContext' (see {@section Request Message})</li>
%%%        </ol></li></ol>
%%%
%%%
%%%      See {@link callback()}.
%%%
%%%      == Request Message ==
%%%
%%%      To ask the `info_service' process to call the `Provider'
%%%      function, the dependency <b>must</b> send Erlang messages
%%%      (rather than invoke API functions) to the info service
%%%      process (registered previously via the `Registration'
%%%      callback) of the following form:
%%%
%%%        `{invoke, ProviderParameters::[term()], HandlerContext::term()}'
%%%
%%%      The `HandlerContext' is a request identifier ignored by
%%%      `riak_core', to be returned to the caller.
%%%
%%%      == History ==
%%%      The information service originates from a need in `eleveldb' to retrieve
%%%      bucket properties.
%%%
%%%      For that particular problem the callbacks would look like this:
%%%  ```
%%%  Registration = {eleveldb, set_metadata_pid, []}
%%%  Provider = {riak_core_bucket, get_bucket, []}
%%%  Handler = {eleveldb, handle_metadata_response, []}
%%%  '''
%%%
%%%      And `handle_metadata_response/1' would look like this,
%%%      assuming `Key' was sent with the original invocation message
%%%      as the 3rd element of the tuple:
%%%  ```
%%%  handle_metadata_response({Props, _ProviderParams, Key}) ->
%%%      property_cache(Key, Props).
%%%
%%%  set_metadata_pid(_Pid) ->
%%%      erlang:nif_error({error, not_loaded}).
%%%  '''


-module(riak_core_info_service).

-export([start_service/4]).


-type callback() :: {module(), FunName::atom(),InitialArgs::[term()]} | undefined.

%% @type callback(). Any arguments provided during registration of
%% `Handler' will be sent as the first parameters when the callback is
%% invoked; the result of the `Provider' callback will be wrapped in a
%% tuple as the last parameter. See {@section Handler Parameters}

-export_type([callback/0]).

-spec start_service(Registration::callback(), Shutdown::callback(), Provider::callback(), Handler::callback()) ->
                           ok |
                           {error, term()}.

start_service(Registration, Shutdown, Provider, Handler) ->
    riak_core_info_service_sup:start_service(Registration, Shutdown, Provider, Handler).
