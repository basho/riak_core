%% -------------------------------------------------------------------
%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
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
%%
%% @doc Throttling for activities that can potentially overload resources.
%% Provides support for "throttling" (i.e. slowing down) calling processes
%% that, when under heavy load, could potentially overload other resources.
%% Activities are identified by a unique key so that different throttle values
%% can be used for different activities.
%%
%% The throttle for an activity can be set to a specific value with the
%% `set_throttle/2' function, or it can be set to a value based on the current
%% "load" for the activity by using the `set_limits/2' function to define a
%% mapping from load factors to throttle values, and then periodically calling
%% the `set_throttle_by_load/2' function with the current load factor. Note
%% that "load factor" is an abstract concept, the purpose of which is simply
%% to represent numerically the amount of load that an activity is under and
%% map that level of load to a particular throttle level. Examples of metrics
%% that could be used for load factors include the size of the mailbox for a
%% particular process, or the number of messages per second being sent to an
%% external service such as Solr.
-module(riak_core_throttle).

%% API
-export([get_throttle/2,
         set_throttle/3,
         clear_throttle/2,
         disable_throttle/2,
         enable_throttle/2,
         is_throttle_enabled/2,
         init/0,
         init/4,
         set_limits/3,
         get_limits/2,
         clear_limits/2,
         create_limits_translator_fun/2,
         set_throttle_by_load/3,
         throttle/2]).

-ifdef(TEST).
-export([get_throttle_for_load/3]).
-define(SLEEP(Time), Time).
-else.
-define(SLEEP(Time), timer:sleep(Time)).
-endif.

-type app_name() :: atom().
-type activity_key() :: atom().
-type throttle_time() :: non_neg_integer().
-type load_factor() :: number() | atom() | tuple().
-type limits() :: [{load_factor(), throttle_time()}].

-export_type([activity_key/0, throttle_time/0, load_factor/0, limits/0]).

-define(ETS_TABLE_NAME, riak_core_throttles).
-define(ETS_TABLE_OPTIONS, [set, named_table, public]).

-define(THROTTLE_KEY(Key), list_to_atom("throttle_" ++ atom_to_list(Key))).
-define(THROTTLE_LIMITS_KEY(Key),
        list_to_atom("throttle_limits_" ++ atom_to_list(Key))).
-define(THROTTLE_ENABLED_KEY(Key),
        list_to_atom("throttle_enabled_" ++ atom_to_list(Key))).

%% @doc Initialize the throttling subsystem. Should be called just once during
%% startup, although calling multiple times will not have any negative
%% consequences.
-spec init() -> ok.
init() ->
    case riak_core_table_owner:maybe_create_table(?ETS_TABLE_NAME, ?ETS_TABLE_OPTIONS) of
        {ok, ?ETS_TABLE_NAME} ->
            ok;
        {error, Reason} ->
            throw({"Failed to create ETS table for riak_core_throttle", Reason})
    end.

%% @doc Sets the throttle for the activity identified by `AppName' and `Key' to
%% the specified `Time'.
-spec set_throttle(app_name(), activity_key(), throttle_time()) -> ok.
set_throttle(AppName, Key, Time) when Time >= 0 ->
    maybe_log_throttle_change(AppName, Key, Time, "client request"),
    do_set_throttle(AppName, Key, Time).

%% @private
%% Actually set the throttle, but don't log anything.
do_set_throttle(AppName, Key, Time) when Time >= 0 ->
    set_value(AppName, ?THROTTLE_KEY(Key), Time).

%% @doc Clears the throttle for the activity identified by `AppName' and `Key'.
-spec clear_throttle(app_name(), activity_key()) -> ok.
clear_throttle(AppName, Key) ->
    unset_value(AppName, ?THROTTLE_KEY(Key)).

%% @doc Disables the throttle for the activity identified by `AppName' and
%% `Key'.
-spec disable_throttle(app_name(), activity_key()) -> ok.
disable_throttle(AppName, Key) ->
    lager:info("Disabling throttle for ~p/~p.", [AppName, Key]),
    set_value(AppName, ?THROTTLE_ENABLED_KEY(Key), false).

%% @doc Enables the throttle for the activity identified by `AppName' and
%% `Key'.
-spec enable_throttle(app_name(), activity_key()) -> ok.
enable_throttle(AppName, Key) ->
    lager:info("Enabling throttle for ~p/~p.", [AppName, Key]),
    set_value(AppName, ?THROTTLE_ENABLED_KEY(Key), true).

%% @doc Returns `true' if the throttle for the activity identified by `AppName'
%% and `Key' is enabled, `false' otherwise.
-spec is_throttle_enabled(app_name(), activity_key()) -> boolean().
is_throttle_enabled(AppName, Key) ->
    get_value(AppName, ?THROTTLE_ENABLED_KEY(Key), true).

%% @doc Initializes the throttle for the activity identified by `AppName' and
%% `Key' using configuration values found in the application environment for
%% `AppName'.
-spec init(AppName::app_name(), Key::activity_key(),
           {LimitsKey::atom(), LimitsDefault::limits()},
           {EnabledKey::atom(), EnabledDefault::boolean()}) ->
    ok | {error, Reason :: term()}.
init(AppName, Key,
     {LimitsKey, LimitsDefault},
     {EnabledKey, EnabledDefault}) ->
    Limits = app_helper:get_env(AppName, LimitsKey, LimitsDefault),
    set_limits(AppName, Key, Limits),
    Enabled = app_helper:get_env(AppName, EnabledKey, EnabledDefault),
    case Enabled of
        true ->
            enable_throttle(AppName, Key);
        false ->
            disable_throttle(AppName, Key)
    end.

%% @doc <p>Sets the limits for the activity identified by `AppName' and `Key'.
%% `Limits' is a mapping from load factors to throttle values. Once this
%% mapping has been established, the `set_throttle_by_load/2' function can
%% be used to set the throttle for the activity based on the current load
%% factor.</p>
%%
%% <p>All of the throttle values in the `Limits' list must be non-negative
%% integers. Additionally, the `Limits' list must contain a tuple with the
%% key `-1'. If either of these conditions does not hold, this function exits
%% with an error.</p>
%% @end
%%
%% @see set_throttle_by_load/2
-spec set_limits(app_name(), activity_key(), limits()) -> ok | no_return().
set_limits(AppName, Key, Limits) ->
    ok = validate_limits(AppName, Key, Limits),
    set_value(AppName, ?THROTTLE_LIMITS_KEY(Key), lists:sort(Limits)).

%% @doc Returns the limits for the activity identified by `AppName' and `Key'
%% if defined, otherwise returns `undefined'.
-spec get_limits(app_name(), activity_key()) -> limits() | undefined.
get_limits(AppName, Key) ->
    case get_value(AppName, ?THROTTLE_LIMITS_KEY(Key)) of
        {ok, Limits} ->
            Limits;
        _ ->
            undefined
    end.

%% @doc Clears the limits for the activity identified by `AppName' and `Key'.
-spec clear_limits(app_name(), activity_key()) -> ok.
clear_limits(AppName, Key) ->
    unset_value(AppName, ?THROTTLE_LIMITS_KEY(Key)).

%% @doc Returns a fun that used in Cuttlefish translations to translate from
%% configuration items of the form:
%%  `ConfigPrefix'.throttle.tier1.`LoadFactorMeasure'
%% to the list of tuples form expected by the `set_limits/2' function in this
%% module. See riak_kv.schema and yokozuna.schema for example usages.
create_limits_translator_fun(ConfigPrefix, LoadFactorMeasure) ->
    fun(Conf) ->
            %% Grab all of the possible names of tiers so we can ensure that
            %% both LoadFactorMeasure and delay are included for each tier.
            CfgPrefix = cuttlefish_variable:tokenize(ConfigPrefix),
            Prefix = flat_concat([CfgPrefix, "throttle", "$tier"]),
            LoadFactorTierNames = cuttlefish_variable:fuzzy_matches(
                                    flat_concat([Prefix, LoadFactorMeasure]),
                                    Conf),
            DelayTierNames = cuttlefish_variable:fuzzy_matches(
                               flat_concat([Prefix, "delay"]), Conf),
            TierNames = lists:usort(LoadFactorTierNames ++ DelayTierNames),

            Throttles = get_throttles(Conf, CfgPrefix, TierNames, LoadFactorMeasure),

            case Throttles of
                %% -1 is a magic "minimum" bound and must be included, so if it
                %% isn't present we call it invalid
                [{-1,_}|_] ->
                    Throttles;
                _ ->
                    Msg = ConfigPrefix
                        ++ ".throttle tiers must include a tier with "
                        ++ LoadFactorMeasure
                        ++ " 0",
                    cuttlefish:invalid(Msg)
            end
    end.

%% @private
%% Concatenates all of the individual elements in `ListOfLists' into a single
%% list, flattening one level if necessary.
flat_concat(ListOfLists) when is_list(ListOfLists)->
    lists:foldl(
      fun(X, Acc) ->
              case length(X) == lists:flatlength(X) of
                  true ->
                      lists:append(Acc, [X]);
                  false ->
                      lists:append(Acc, X)
              end
      end,
      [],
      ListOfLists).

get_throttles(Conf, ConfigPrefix, TierNames, LoadFactorMeasure) ->
    lists:sort(
      lists:foldl(
        fun({"$tier", Tier}, Settings) ->
                TierPrefix = flat_concat([ConfigPrefix, "throttle", Tier]),
                LoadFactorValue = cuttlefish:conf_get(
                                    flat_concat([TierPrefix, LoadFactorMeasure]),
                                    Conf),
                Delay =
                cuttlefish:conf_get( flat_concat([TierPrefix, "delay"]), Conf),
                [{LoadFactorValue - 1, Delay} | Settings]
        end,
        [],
        TierNames)).

%% @doc <p>Sets the throttle for the activity identified by `AppName' and `Key'
%% to a value determined by consulting the limits for the activity. The
%% throttle value is the value associated with the largest
%% load factor that is less than or equal to `LoadFactor'.
%% Normally the `LoadFactor' will be a number representing the current level
%% of load for the activity, but it is also allowed to pass an atom or a tuple
%% as the `LoadFactor'. This can be used when a numeric value cannot be
%% established, and results in using the throttle value for the largest load
%% factor defined in the limits for the activity.</p>
%%
%% <p>If there are no limits defined for the activity, exits with
%% error({no_limits, Key}).</p>
%% @see set_limits/2
-spec set_throttle_by_load(app_name(), activity_key(), load_factor()) ->
    throttle_time().
set_throttle_by_load(AppName, Key, LoadFactor) ->
    case get_throttle_for_load(AppName, Key, LoadFactor) of
        undefined ->
            error({no_limits, AppName, Key});
        ThrottleVal ->
            ThrottleReason = lists:flatten(io_lib:format("load factor ~p",
                [LoadFactor])),
            maybe_log_throttle_change(AppName, Key, ThrottleVal, ThrottleReason),
            do_set_throttle(AppName, Key, ThrottleVal),
            ThrottleVal
    end.

%% @doc Sleep for the number of milliseconds specified as the throttle time for
%% the activity identified by `AppName' and `Key' (unless the throttle for the
%% activity has been disabled with `disable_throttle/1'). If there is no
%% throttle value configured for `Key', then exits with error({badkey, Key}).
-spec throttle(app_name(), activity_key()) -> throttle_time().
throttle(AppName, Key) ->
    maybe_throttle(Key,
                   get_throttle(AppName, Key),
                   is_throttle_enabled(AppName, Key)).

maybe_throttle(Key, undefined, _False) ->
    error({badkey, Key});
maybe_throttle(_Key, _Time, false) ->
    0;
maybe_throttle(_Key, 0, _Enabled) ->
    0;
maybe_throttle(_Key, Time, true) ->
    ?SLEEP(Time),
    Time.

-spec get_throttle(app_name(), activity_key()) -> throttle_time() | undefined.
get_throttle(AppName, Key) ->
    get_value(AppName, ?THROTTLE_KEY(Key), undefined).

-spec get_throttle_for_load(app_name(), activity_key(), load_factor()) ->
    throttle_time() | undefined.
get_throttle_for_load(AppName, Key, LoadFactor) ->
    case get_limits(AppName, Key) of
        undefined ->
            undefined;
        Limits ->
            find_throttle_for_load_factor(Limits, LoadFactor)
    end.

%% The current structure (defined by riak_kv_entropy_manager) of the Limits
%% list is like this:
%% [{-1,0}, {200,10}, {500,50}, {750,250}, {900,1000}, {1100,5000}].
%% where in each tuple, you have {LoadGreaterThan, ThrottleVal}.
%% If, instead, we eliminated the magical "-1" value and made each entry
%% {LoadLessThan, ThrottleVal} instead, we could manipulate this
%% structure much more easily. Unfortunately, it would change the
%% advanced.config structure in a way that will affect existing customers.
%% We're investigating changing this.
find_throttle_for_load_factor([], _LoadFactor) ->
    undefined;
find_throttle_for_load_factor(Limits, max) ->
    {_Load, ThrottleVal} = lists:last(Limits),
    ThrottleVal;
find_throttle_for_load_factor([{_Load, ThrottleVal}, {NextLoad, _NextThrottleVal}|_], LoadFactor)
    when is_number(LoadFactor), LoadFactor < NextLoad ->
    ThrottleVal;
find_throttle_for_load_factor([{_, ThrottleVal}], LoadFactor) when is_number(LoadFactor) ->
    ThrottleVal;
find_throttle_for_load_factor([_|Limits], LoadFactor) when is_number(LoadFactor) ->
    find_throttle_for_load_factor(Limits, LoadFactor).

validate_limits(AppName, Key, Limits) ->
    Validators = [validate_all_non_negative(Limits),
                  validate_has_negative_one_key(Limits)],
    Errors = [Message || {error, Message} <- Validators],
    case Errors of
        [] ->
            ok;
        Messages ->
            lager:error("Invalid throttle limits for application ~p, activity ~p: ~p.",
                        [AppName, Key, Messages]),
            error(invalid_throttle_limits)
    end.

validate_all_non_negative(Limits) ->
    Good = lists:all(
      fun({LoadFactor, ThrottleVal}) ->
              is_integer(LoadFactor) andalso
              is_integer(ThrottleVal) andalso
              ThrottleVal >= 0
      end,
      Limits),
    case Good of
        true ->
            ok;
        false ->
            {error, "All throttle values must be non-negative integers"}
    end.

validate_has_negative_one_key(Limits) ->
    case lists:keyfind(-1, 1, Limits) of
        {-1, _} ->
            ok;
        false ->
            {error, "Must include -1 entry"}
    end.

maybe_log_throttle_change(AppName, Key, NewValue, Reason) ->
    OldValue = get_throttle(AppName, Key),
    case NewValue == OldValue of
        true ->
            ok;
        false ->
            lager:info("Changing throttle for ~p/~p from ~p to ~p based on ~ts",
                       [AppName, Key, OldValue, NewValue, Reason])
    end.

set_value(AppName, Key, Value) ->
    true = ets:insert(?ETS_TABLE_NAME, {{AppName, Key}, Value}),
    ok.

unset_value(AppName, Key) ->
    true = ets:delete(?ETS_TABLE_NAME, {AppName, Key}),
    ok.

get_value(AppName, Key) ->
    case ets:lookup(?ETS_TABLE_NAME, {AppName, Key}) of
        [] ->
            {error, undefined};
        [{{AppName, Key}, Value}] ->
            {ok, Value}
    end.

get_value(AppName, Key, Default) ->
    case get_value(AppName, Key) of
        {ok, Value} ->
            Value;
        _ ->
            Default
    end.
