%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc OTP equivalents for sending reply- and event-like things
%% without blocking the sender.

-module(riak_core_send_msg).

-export([reply_unreliable/2,
         cast_unreliable/2,
         send_event_unreliable/2,
         bang_unreliable/2]).

-ifdef(TEST).
-ifdef(PULSE).
-compile(export_all).
-compile({parse_transform, pulse_instrument}).
-compile({pulse_replace_module, [{gen_fsm, pulse_gen_fsm},
                                 {gen_fsm_compat, pulse_gen_fsm},
                                 {gen_server, pulse_gen_server}]}).
-endif.
-endif.

%% NOTE: We'ed peeked inside gen_server.erl's guts to see its internals.
reply_unreliable({To, Tag}, Reply) ->
    bang_unreliable(To, {Tag, Reply}).

cast_unreliable(Dest, Request) ->
    bang_unreliable(Dest, {'$gen_cast', Request}).

%% NOTE: We'ed peeked inside gen_fsm.erl's guts to see its internals.
send_event_unreliable({global, _Name} = GlobalTo, Event) ->
    erlang:error({unimplemented_send, GlobalTo, Event});
send_event_unreliable({via, _Mod, _Name} = ViaTo, Event) ->
    erlang:error({unimplemented_send, ViaTo, Event});
send_event_unreliable(Name, Event) ->
    bang_unreliable(Name, {'$gen_event', Event}),
    ok.

bang_unreliable(Dest, Msg) ->
    catch erlang:send(Dest, Msg, [noconnect, nosuspend]),
    Msg.
