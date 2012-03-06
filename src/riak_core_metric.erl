%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc riak_core_metric is a behaviour that metrics conform to.
%% it is part of the riak_core stats subsystem.
%% current impls are Meter(spiral), Counter, Histogram(slide)

-module(riak_core_metric).

-export([behaviour_info/1, regname/2]).

-export_type([stat_specs/0]).

-type stat_specs() ::  [stat()].

-type stat() :: {Name :: atom(),
                 Args :: [stat_arg()]
                }.

-type stat_arg() :: {Name :: type | desc | display,
                     Value :: term()
                    }.

behaviour_info(callbacks) ->
    [{new, 0},
     {value, 1},
     {value, 2},
     {update, 2}
    ].

regname(App, Name) when is_atom(App), is_atom(Name) ->
    AppBin = atom_to_binary(App, latin1),
    NameBin = atom_to_binary(Name, latin1),
    AllBin = <<$s,$t,$a,$t,$s,$_, AppBin/binary, $_, NameBin/binary>>,
    binary_to_atom(AllBin, latin1).

