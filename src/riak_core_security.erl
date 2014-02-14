%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
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
-module(riak_core_security).

%% printing functions
-export([print_users/0, print_sources/0, print_user/1,
         print_groups/0, print_group/1]).

%% API
-export([authenticate/3, add_user/2, alter_user/2, del_user/1,
         add_group/2, alter_group/2, del_group/1,
         add_source/4, del_source/2,
         add_grant/3, add_revoke/3, check_permission/2, check_permissions/2,
         get_username/1, is_enabled/0, enable/0, disable/0, status/0,
         get_ciphers/0, set_ciphers/1, print_ciphers/0]).
%% TODO add rm_source, API to deactivate/remove users

-define(DEFAULT_CIPHER_LIST,
"ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES128-GCM-SHA256"
":ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-AES256-GCM-SHA384"
":DHE-RSA-AES128-GCM-SHA256:DHE-DSS-AES128-GCM-SHA256"
":DHE-DSS-AES256-GCM-SHA384:DHE-RSA-AES256-GCM-SHA384"
":ADH-AES256-GCM-SHA384:ADH-AES128-GCM-SHA256"
":ECDHE-RSA-AES128-SHA256:ECDHE-ECDSA-AES128-SHA256"
":ECDHE-RSA-AES128-SHA:ECDHE-ECDSA-AES128-SHA:ECDHE-RSA-AES256-SHA384"
":ECDHE-ECDSA-AES256-SHA384:ECDHE-RSA-AES256-SHA:ECDHE-ECDSA-AES256-SHA"
":DHE-RSA-AES128-SHA256:DHE-RSA-AES128-SHA:DHE-DSS-AES128-SHA256"
":DHE-RSA-AES256-SHA256:DHE-DSS-AES256-SHA:DHE-RSA-AES256-SHA"
":AES128-GCM-SHA256:AES256-GCM-SHA384:ECDHE-RSA-RC4-SHA:ECDHE-ECDSA-RC4-SHA"
":SRP-DSS-AES-128-CBC-SHA:SRP-RSA-AES-128-CBC-SHA:DHE-DSS-AES128-SHA"
":AECDH-AES128-SHA:SRP-AES-128-CBC-SHA:ADH-AES128-SHA256:ADH-AES128-SHA"
":ECDH-RSA-AES128-GCM-SHA256:ECDH-ECDSA-AES128-GCM-SHA256"
":ECDH-RSA-AES128-SHA256:ECDH-ECDSA-AES128-SHA256:ECDH-RSA-AES128-SHA"
":ECDH-ECDSA-AES128-SHA:AES128-SHA256:AES128-SHA:SRP-DSS-AES-256-CBC-SHA"
":SRP-RSA-AES-256-CBC-SHA:DHE-DSS-AES256-SHA256:AECDH-AES256-SHA"
":SRP-AES-256-CBC-SHA:ADH-AES256-SHA256:ADH-AES256-SHA"
":ECDH-RSA-AES256-GCM-SHA384:ECDH-ECDSA-AES256-GCM-SHA384"
":ECDH-RSA-AES256-SHA384:ECDH-ECDSA-AES256-SHA384:ECDH-RSA-AES256-SHA"
":ECDH-ECDSA-AES256-SHA:AES256-SHA256:AES256-SHA:RC4-SHA"
":DHE-RSA-CAMELLIA256-SHA:DHE-DSS-CAMELLIA256-SHA:ADH-CAMELLIA256-SHA"
":CAMELLIA256-SHA:DHE-RSA-CAMELLIA128-SHA:DHE-DSS-CAMELLIA128-SHA"
":ADH-CAMELLIA128-SHA:CAMELLIA128-SHA").

-define(TOMBSTONE, '$deleted').

-record(context,
        {username,
         grants,
         epoch}).

prettyprint_users([all], _) ->
    "all";
prettyprint_users(Users0, Width) ->
    %% my kingdom for an iolist join...
    Users = [binary_to_list(U) || U <- Users0],
    prettyprint_permissions(Users, Width).

print_sources() ->
    Sources = riak_core_metadata:fold(fun({{Username, CIDR}, [{Source, Options}]}, Acc) ->
                                              [{Username, CIDR, Source, Options}|Acc];
                                         ({{_, _}, [?TOMBSTONE]}, Acc) ->
                                              Acc
                                      end, [], {<<"security">>, <<"sources">>}),

    print_sources(Sources).

print_sources(Sources) ->
    GS = group_sources(Sources),
    riak_core_console_table:print([{users, 20}, {cidr, 10}, {source, 10}, {options, 10}],
                [[prettyprint_users(Users, 20), prettyprint_cidr(CIDR),
                  atom_to_list(Source), io_lib:format("~p", [Options])] ||
            {Users, CIDR, Source, Options} <- GS]).

print_users() ->
    Users = riak_core_metadata:fold(fun({_Username, [?TOMBSTONE]}, Acc) ->
                                            Acc;
                                        ({Username, Options}, Acc) ->
                                    [{Username, Options}|Acc]
                            end, [], {<<"security">>, <<"users">>}),
    riak_core_console_table:print([{username, 10}, {'member of', 15}, {password, 40}, {options, 30}],
                [begin
                     Groups = case proplists:get_value("groups", Options) of
                                 undefined ->
                                     "";
                                 List ->
                                     prettyprint_permissions([binary_to_list(R)
                                                              || R <- List,
                                                                 group_exists(R)], 20)
                             end,
                     Password = case proplists:get_value("password", Options) of
                                    undefined ->
                                        "";
                                    Pw ->
                                        proplists:get_value(hash_pass, Pw)
                                end,
                     OtherOptions = lists:keydelete("password", 1,
                                                    lists:keydelete("groups", 1,
                                                                    Options)),
                     [Username, Groups, Password,
                      lists:flatten(io_lib:format("~p", [OtherOptions]))]
                 end ||
            {Username, [Options]} <- Users]).

print_groups() ->
    Groups = riak_core_metadata:fold(fun({_Groupname, [?TOMBSTONE]}, Acc) ->
                                             Acc;
                                        ({Groupname, Options}, Acc) ->
                                    [{Groupname, Options}|Acc]
                            end, [], {<<"security">>, <<"groups">>}),
    riak_core_console_table:print([{group, 10}, {'member of', 15}, {options, 30}],
                [begin
                     GroupOptions = case proplists:get_value("groups", Options) of
                                 undefined ->
                                     "";
                                 List ->
                                     prettyprint_permissions([binary_to_list(R)
                                                              || R <- List,
                                                                 group_exists(R)], 20)
                             end,
                     OtherOptions = lists:keydelete("groups", 1, Options),
                     [Groupname, GroupOptions,
                      lists:flatten(io_lib:format("~p", [OtherOptions]))]
                 end ||
            {Groupname, [Options]} <- Groups]).

print_user(User) ->
    case user_details(User) of
        undefined ->
            io:format("No such user ~p~n", [User]),
            {error, {unknown_user, User}};
        _U ->
            Grants = accumulate_grants(User),
            io:format("~nInherited permissions~n~n"),
            riak_core_console_table:print([{group, 20}, {type, 10}, {bucket, 10}, {grants, 40}],
                        [begin
                             case Bucket of
                                 any ->
                                     [Username, "*", "*",
                                      prettyprint_permissions(Permissions, 40)];
                                 {T, B} ->
                                     [Username, T, B,
                                      prettyprint_permissions(Permissions, 40)];
                                 T ->
                                     [Username, T, "*",
                                      prettyprint_permissions(Permissions, 40)]
                             end
                         end ||
                         {{Username, Bucket}, Permissions} <- Grants, Username /= User]),
            GroupedGrants = group_grants(Grants),
            io:format("~nApplied permissions~n~n"),
            riak_core_console_table:print([{type, 10}, {bucket, 10}, {grants, 40}],
                        [begin
                             case Bucket of
                                 any ->
                                     ["*", "*",
                                      prettyprint_permissions(Permissions, 40)];
                                 {T, B} ->
                                     [T, B,
                                      prettyprint_permissions(Permissions, 40)];
                                 T ->
                                     [T, "*",
                                      prettyprint_permissions(Permissions, 40)]
                             end
                         end ||
                         {Bucket, Permissions} <- GroupedGrants]),
            ok
    end.

print_group(Group) ->
    case group_details(Group) of
        undefined ->
            io:format("No such group ~p~n", [Group]),
            {error, {unknown_group, Group}};
        _U ->
            Grants = accumulate_grants(Group),
            io:format("~nInherited permissions~n~n"),
            riak_core_console_table:print([{group, 20}, {type, 10}, {bucket, 10}, {grants, 40}],
                        [begin
                             case Bucket of
                                 any ->
                                     [Groupname, "*", "*",
                                      prettyprint_permissions(Permissions, 40)];
                                 {T, B} ->
                                     [Groupname, T, B,
                                      prettyprint_permissions(Permissions, 40)];
                                 T ->
                                     [Groupname, T, "*",
                                      prettyprint_permissions(Permissions, 40)]
                             end
                         end ||
                         {{Groupname, Bucket}, Permissions} <- Grants, Groupname /= Group]),
            GroupedGrants = group_grants(Grants),
            io:format("~nApplied permissions~n~n"),
            riak_core_console_table:print([{type, 10}, {bucket, 10}, {grants, 40}],
                        [begin
                             case Bucket of
                                 any ->
                                     ["*", "*",
                                      prettyprint_permissions(Permissions, 40)];
                                 {T, B} ->
                                     [T, B,
                                      prettyprint_permissions(Permissions, 40)];
                                 T ->
                                     [T, "*",
                                      prettyprint_permissions(Permissions, 40)]
                             end
                         end ||
                         {Bucket, Permissions} <- GroupedGrants]),
            ok
    end.

prettyprint_permissions(Permissions, Width) ->
    prettyprint_permissions(lists:sort(Permissions), Width, []).

prettyprint_permissions([], _Width, Acc) ->
    string:join([string:join(Line, ", ") || Line <- lists:reverse(Acc)], ",\n");
prettyprint_permissions([Permission|Rest], Width, [H|T] =Acc) ->
    case length(Permission) + lists:flatlength(H) + 2 + (2 * length(H)) > Width of
        true ->
            prettyprint_permissions(Rest, Width, [[Permission] | Acc]);
        false ->
            prettyprint_permissions(Rest, Width, [[Permission|H]|T])
    end;
prettyprint_permissions([Permission|Rest], Width, Acc) ->
    prettyprint_permissions(Rest, Width, [[Permission] | Acc]).

check_permission({Permission}, Context0) ->
    Context = maybe_refresh_context(Context0),
    %% The user needs to have this permission applied *globally*
    %% This is for things like mapreduce with undetermined inputs or
    %% permissions that don't tie to a particular bucket, like 'ping' and
    %% 'stats'.
    MatchG = match_grant(any, Context#context.grants),
    case MatchG /= undefined andalso
         (lists:member(Permission, MatchG) orelse MatchG == 'all') of
        true ->
            {true, Context};
        false ->
            %% no applicable grant
            {false, io_lib:format("Permission denied: User '~s' does not have "
                                  "'~s' on ANY", [Context#context.username,
                                                 Permission]), Context}
    end;
check_permission({Permission, Bucket}, Context0) ->
    Context = maybe_refresh_context(Context0),
    MatchG = match_grant(Bucket, Context#context.grants),
    case MatchG /= undefined andalso
         (lists:member(Permission, MatchG) orelse MatchG == 'all') of
        true ->
            {true, Context};
        false ->
            %% no applicable grant
            {false, io_lib:format("Permission denied: User '~s' does not have "
                                  "'~s' on ~p", [Context#context.username,
                                                 Permission,
                                                 Bucket]), Context}
    end.

check_permissions(Permission, Ctx) when is_tuple(Permission) ->
    %% single permission
    check_permission(Permission, Ctx);
check_permissions([], Ctx) ->
    {true, Ctx};
check_permissions([Permission|Rest], Ctx) ->
    case check_permission(Permission, Ctx) of
        {true, NewCtx} ->
            check_permissions(Rest, NewCtx);
        Other ->
            %% return non-standard result
            Other
    end.

get_username(#context{username=Username}) ->
    Username.

authenticate(Username, Password, ConnInfo) ->
    case user_details(Username) of
        undefined ->
            {error, unknown_user};
        UserData ->
            Sources0 = riak_core_metadata:fold(fun({{Un, CIDR}, [{Source, Options}]}, Acc) ->
                                                      [{Un, CIDR, Source, Options}|Acc];
                                                  ({{_, _}, [?TOMBSTONE]}, Acc) ->
                                                       Acc
                                              end, [], {<<"security">>, <<"sources">>}),
            Sources = sort_sources(Sources0),
            case match_source(Sources, Username,
                              proplists:get_value(ip, ConnInfo)) of
                {ok, Source, SourceOptions} ->
                    case Source of
                        trust ->
                            %% trust always authenticates
                            {ok, get_context(Username)};
                        password ->
                            %% pull the password out of the userdata
                            case lookup("password", UserData) of
                                undefined ->
                                    lager:warning("User ~p is configured for "
                                                  "password authentication, but has "
                                                  "no password", [Username]),
                                    {error, missing_password};
                                PasswordData ->
                                    HashedPass = lookup(hash_pass, PasswordData),
                                    HashFunction = lookup(hash_func, PasswordData),
                                    Salt = lookup(salt, PasswordData),
                                    Iterations = lookup(iterations, PasswordData),
                                    case riak_core_pw_auth:check_password(Password,
                                                                          HashedPass,
                                                                          HashFunction,
                                                                          Salt,
                                                                          Iterations) of
                                        true ->
                                            {ok, get_context(Username)};
                                        false ->
                                            {error, bad_password}
                                    end
                            end;
                        certificate ->
                            case proplists:get_value(common_name, ConnInfo) of
                                undefined ->
                                    {error, no_common_name};
                                CN ->
                                    %% TODO postgres support a map from
                                    %% common-name to username, should we?
                                    case list_to_binary(CN) == Username of
                                        true ->
                                            {ok, get_context(Username)};
                                        false ->
                                            {error, common_name_mismatch}
                                    end
                            end;
                        Source ->
                            %% check for a dynamically registered auth module
                            AuthMods = app_helper:get_env(riak_core,
                                                          auth_mods, []),
                            case proplists:get_value(Source, AuthMods) of
                                undefined ->
                                    lager:warning("User ~p is configured with unknown "
                                                  "authentication source ~p",
                                                  [Username, Source]),
                                    {error, unknown_source};
                                AuthMod ->
                                    case AuthMod:auth(Username, Password,
                                                      UserData, SourceOptions) of
                                        ok ->
                                            {ok, get_context(Username)};
                                        error ->
                                            {error, bad_password}
                                    end
                            end
                    end;
                {error, Reason} ->
                    {error, Reason}
            end
    end.

add_user(Username, Options) ->
    case user_exists(Username) of
        false ->
            case validate_options(Options) of
                {ok, NewOptions} ->
                    riak_core_metadata:put({<<"security">>, <<"users">>},
                                           Username, NewOptions),
                    ok;
                Error ->
                    Error
            end;
        true ->
            {error, user_exists}
    end.

add_group(Groupname, Options) ->
    case group_exists(Groupname) of
        false ->
            case validate_groups_option(Options) of
                {ok, NewOptions} ->
                    riak_core_metadata:put({<<"security">>, <<"groups">>},
                                           Groupname, NewOptions),
                    ok;
                Error ->
                    Error
            end;
        true ->
            {error, group_exists}
    end.

alter_user(Username, Options) ->
    case user_details(Username) of
        undefined ->
            {error, {unknown_user, Username}};
        UserData ->
            case validate_options(Options) of
                {ok, NewOptions} ->
                    MergedOptions = lists:ukeymerge(1, lists:sort(NewOptions),
                                                    lists:sort(UserData)),

                    riak_core_metadata:put({<<"security">>, <<"users">>},
                                           Username, MergedOptions),
                    ok;
                Error ->
                    Error
            end
    end.

alter_group(Groupname, Options) ->
    case group_details(Groupname) of
        undefined ->
            {error, {unknown_group, Groupname}};
        GroupData ->
            case validate_groups_option(Options) of
                {ok, NewOptions} ->
                    MergedOptions = lists:ukeymerge(1, lists:sort(NewOptions),
                                                    lists:sort(GroupData)),

                    riak_core_metadata:put({<<"security">>, <<"groups">>},
                                           Groupname, MergedOptions),
                    ok;
                Error ->
                    Error
            end
    end.

del_user(Username) ->
    case user_exists(Username) of
        false ->
            {error, {unknown_user, Username}};
        true ->
            riak_core_metadata:delete({<<"security">>, <<"users">>},
                                   Username),
            %% delete any associated grants, so if a user with the same name
            %% is added again, they don't pick up these grants
            riak_core_metadata:fold(fun({Key, _Value}, Acc) ->
                                            %% apparently destructive
                                            %% iteration is allowed
                                            riak_core_metadata:delete({<<"security">>,
                                                                       <<"grants">>},
                                                                       Key),
                                            Acc
                                    end, undefined,
                                    {<<"security">> ,<<"grants">>},
                                    [{match, {Username, '_'}}]),
            delete_user_from_sources(Username),
            ok
    end.

del_group(Groupname) ->
    case group_exists(Groupname) of
        false ->
            {error, {unknown_group, Groupname}};
        true ->
            riak_core_metadata:delete({<<"security">>, <<"groups">>},
                                   Groupname),
            %% delete any associated grants, so if a user with the same name
            %% is added again, they don't pick up these grants
            riak_core_metadata:fold(fun({Key, _Value}, Acc) ->
                                            %% apparently destructive
                                            %% iteration is allowed
                                            riak_core_metadata:delete({<<"security">>,
                                                                       <<"grants">>},
                                                                       Key),
                                            Acc
                                    end, undefined,
                                    {<<"security">> ,<<"grants">>},
                                    [{match, {Groupname, '_'}}]),
            delete_group_from_roles(Groupname),
            ok
    end.

add_grant(all, Bucket, Grants) ->
    %% all is always valid
    case validate_permissions(Grants) of
        ok ->
            add_grant_int([all], Bucket, Grants);
        Error ->
            Error
    end;
add_grant([H|_T]=RoleList, Bucket, Grants) when is_binary(H) ->
    %% list of lists, weeeee
    %% validate the users...

    UnknownRoles = unknown_roles(RoleList),

    Valid = case UnknownRoles of
                [] ->
                    ok;
                _ ->
                    {error, {unknown_roles, UnknownRoles}}
            end,

    Valid2 = case Valid of
        ok ->
            validate_permissions(Grants);
        Other ->
            Other
    end,
    case Valid2 of
        ok ->
            %% add a grant for each role
            add_grant_int(RoleList, Bucket, Grants);
        Error ->
            Error
    end;
add_grant(Role, Bucket, Grants) ->
    %% single role
    add_grant([Role], Bucket, Grants).


add_revoke(all, Bucket, Revokes) ->
    %% all is always valid
    case validate_permissions(Revokes) of
        ok ->
            case add_revoke_int([all], Bucket, revokes) of
                ok ->
                    ok;
                Error2 ->
                    Error2
            end;
        Error ->
            Error
    end;
add_revoke([H|_T]=RoleList, Bucket, Revokes) when is_binary(H) ->
    %% list of lists, weeeee
    %% validate the role...
    UnknownRoles = unknown_roles(RoleList),
    Valid = case UnknownRoles of
                [] ->
                    ok;
                _ ->
                    {error, {unknown_roles, UnknownRoles}}
            end,
    Valid2 = case Valid of
        ok ->
            validate_permissions(Revokes);
        Other ->
            Other
    end,
    case Valid2 of
        ok ->
            %% revoke any matching grant for each role
            case add_revoke_int(RoleList, Bucket, Revokes) of
                ok ->
                    ok;
                Error2 ->
                    Error2
            end;
        Error ->
            Error
    end;
add_revoke(User, Bucket, Revokes) ->
    %% single user
    add_revoke([User], Bucket, Revokes).


add_source(all, CIDR, Source, Options) ->
    %% all is always valid

    %% TODO check if there are already 'user' sources for this CIDR
    %% with the same source
    riak_core_metadata:put({<<"security">>, <<"sources">>},
                           {all, anchor_mask(CIDR)},
                           {Source, Options}),
    ok;
add_source([H|_T]=UserList, CIDR, Source, Options) when is_binary(H) ->
    %% list of lists, weeeee
    %% validate the users...

    %% We only allow sources to be assigned to users, so don't check
    %% for valid group names
    UnknownUsers = unknown_roles(UserList, user),

    Valid = case UnknownUsers of
                [] ->
                    %% TODO check if there is already an 'all' source for this CIDR
                    %% with the same source
                    ok;
                _ ->
                    {error, {unknown_users, UnknownUsers}}
            end,

    case Valid of
        ok ->
            %% add a source for each user
            add_source_int(UserList, anchor_mask(CIDR), Source,
                                    Options),
            ok;
        Error ->
            Error
    end;
add_source(User, CIDR, Source, Options) ->
    %% single user
    add_source([User], CIDR, Source, Options).

del_source(all, CIDR) ->
    %% all is always valid
    riak_core_metadata:delete({<<"security">>, <<"sources">>},
                              {all, anchor_mask(CIDR)}),
    ok;
del_source([H|_T]=UserList, CIDR) when is_binary(H) ->
    [riak_core_metadata:delete({<<"security">>, <<"sources">>},
                              {User, anchor_mask(CIDR)}) || User <- UserList],
    ok;
del_source(User, CIDR) ->
    %% single user
    del_source([User], CIDR).


is_enabled() ->
    try riak_core_capability:get({riak_core, security}) of
        true ->
           case  riak_core_metadata:get({<<"security">>, <<"status">>},
                                        enabled) of
               true ->
                   true;
               _ ->
                   false
           end;
        _ ->
            false
    catch
        throw:{unknown_capability, {riak_core, security}} ->
            false
    end.

enable() ->
    case riak_core_capability:get({riak_core, security}) of
        true ->
           riak_core_metadata:put({<<"security">>, <<"status">>},
                                        enabled, true);
        false ->
            not_supported
    end.

get_ciphers() ->
    case riak_core_metadata:get({<<"security">>, <<"config">>}, ciphers) of
        undefined ->
            ?DEFAULT_CIPHER_LIST;
        Result ->
            Result
    end.

print_ciphers() ->
    Ciphers = get_ciphers(),
    {Good, Bad} = riak_core_ssl_util:parse_ciphers(Ciphers),
    io:format("Configured ciphers~n~n~s~n~n", [Ciphers]),
    io:format("Valid ciphers(~b)~n~n~s~n~n",
              [length(Good), riak_core_ssl_util:print_ciphers(Good)]),
    case Bad of
        [] ->
            ok;
        _ ->
            io:format("Unknown/Unsupported ciphers(~b)~n~n~s~n~n",
                      [length(Bad), string:join(Bad, ":")])
    end.

set_ciphers(CipherList) ->
    case riak_core_ssl_util:parse_ciphers(CipherList) of
        {[], _} ->
            %% no valid ciphers
            io:format("No known or supported ciphers in list.~n"),
            error;
        _ ->
            riak_core_metadata:put({<<"security">>, <<"config">>}, ciphers,
                                   CipherList),
            ok
    end.

disable() ->
    riak_core_metadata:put({<<"security">>, <<"status">>},
                           enabled, false).

status() ->
    Enabled = riak_core_metadata:get({<<"security">>, <<"status">>}, enabled,
                                    [{default, false}]),
    case Enabled of
        true ->
            case riak_core_capability:get({riak_core, security}) of
                true ->
                    enabled;
                _ ->
                    enabled_but_no_capability
            end;
        _ ->
            disabled
    end.

%% ============
%% INTERNAL
%% ============

add_revoke_int([], _, _) ->
    ok;
add_revoke_int([Role|Roles], Bucket, Permissions) ->
    RoleGrants = riak_core_metadata:get({<<"security">>, <<"grants">>}, {Role,
                                                                         Bucket}),

    %% check if there is currently a GRANT we can revoke
    case RoleGrants of
        undefined ->
            %% can't REVOKE what wasn't GRANTED
            add_revoke_int(Roles, Bucket, Permissions);
        GrantedPermissions ->
            NewPerms = [X || X <- GrantedPermissions, not lists:member(X,
                                                                       Permissions)],

            %% TODO - do deletes here, once cluster metadata supports it for
            %% real, if NeePerms == []
            
            case NewPerms of
                [] ->
                    riak_core_metadata:delete({<<"security">>, <<"grants">>},
                                           {Role, Bucket});
                _ ->
                    riak_core_metadata:put({<<"security">>, <<"grants">>},
                                           {Role, Bucket}, NewPerms)
            end,
            add_revoke_int(Roles, Bucket, Permissions)
    end.

add_source_int([], _, _, _) ->
    ok;
add_source_int([User|Users], CIDR, Source, Options) ->
    riak_core_metadata:put({<<"security">>, <<"sources">>}, {User, CIDR},
                           {Source, Options}),
    add_source_int(Users, CIDR, Source, Options).

add_grant_int([], _, _) ->
    ok;
add_grant_int([Role|Roles], Bucket, Permissions) ->
    BucketPermissions = case riak_core_metadata:get({<<"security">>, <<"grants">>},
                                               {Role, Bucket}) of
                            undefined ->
                                [];
                            Perms ->
                                Perms
                        end,
    NewPerms = lists:umerge(lists:sort(BucketPermissions),
                            lists:sort(Permissions)),
    riak_core_metadata:put({<<"security">>, <<"grants">>}, {Role, Bucket},
                           NewPerms),
    add_grant_int(Roles, Bucket, Permissions).

match_grant(Bucket, Grants) ->
    AnyGrants = proplists:get_value(any, Grants, []),
    %% find the first grant that matches the bucket name and then merge in the
    %% 'any' grants, if any
    lists:umerge(lists:sort(lists:foldl(fun({B, P}, Acc) when Bucket == B ->
                        P ++ Acc;
                   ({B, P}, Acc) when element(1, Bucket) == B ->
                        %% wildcard match against bucket type
                        P ++ Acc;
                   (_, Acc) ->
                        Acc
                end, [], Grants)), lists:sort(AnyGrants)).

maybe_refresh_context(Context) ->
    %% TODO replace this with a cluster metadata hash check, or something
    Epoch = os:timestamp(),
    case timer:now_diff(Epoch, Context#context.epoch) < 1000 of
        false ->
            %% context has expired
            get_context(Context#context.username);
        _ ->
            Context
    end.



%% Contexts are only valid until the GRANT epoch changes, and it will change
%% whenever a GRANT or a REVOKE is performed. This is a little coarse grained
%% right now, but it'll do for the moment.
get_context(Username) when is_binary(Username) ->
    Grants = group_grants(accumulate_grants(Username)),
    #context{username=Username, grants=Grants, epoch=os:timestamp()}.

accumulate_grants(Role) ->
    {Grants, _Seen} = accumulate_grants([Role], [], []),
    lists:flatten(Grants).

accumulate_grants([], Seen, Acc) ->
    {Acc, Seen};
accumulate_grants([Role|Roles], Seen, Acc) ->
    Options = role_details(Role),
    Groups = [G || G <- lookup("groups", Options, []),
                        not lists:member(G,Seen),
                        group_exists(G)],
    {NewAcc, NewSeen} = accumulate_grants(Groups, [Role|Seen], Acc),

    Grants = riak_core_metadata:fold(fun({{_R, _Bucket}, [?TOMBSTONE]}, A) ->
                                             A;
                                        ({{R, Bucket}, [Permissions]}, A) ->
                                             [{{R, Bucket}, Permissions}|A]
                                     end, [], {<<"security">>, <<"grants">>},
                                     [{match, {Role, '_'}}]),
    accumulate_grants(Roles, NewSeen, [Grants|NewAcc]).

%% lookup a key in a list of key/value tuples. Like proplists:get_value but
%% faster.
lookup(Key, List, Default) ->
    case lists:keyfind(Key, 1, List) of
        false ->
            Default;
        {Key, Value} ->
            Value
    end.

lookup(Key, List) ->
    lookup(Key, List, undefined).

stash(Key, Value, List) ->
    lists:keystore(Key, 1, List, Value).

%% @doc Get the subnet mask as an integer, stolen from an old post on
%%      erlang-questions.
mask_address(Addr={_, _, _, _}, Maskbits) ->
    B = list_to_binary(tuple_to_list(Addr)),
    <<Subnet:Maskbits, _Host/bitstring>> = B,
    Subnet;
mask_address({A, B, C, D, E, F, G, H}, Maskbits) ->
    <<Subnet:Maskbits, _Host/bitstring>> = <<A:16, B:16, C:16, D:16, E:16,
                                             F:16, G:16, H:16>>,
    Subnet.

%% @doc returns the real bottom of a netmask. Eg if 192.168.1.1/16 is
%% provided, return 192.168.0.0/16
anchor_mask(Addr={_, _, _, _}, Maskbits) ->
    M = mask_address(Addr, Maskbits),
    Rem = 32 - Maskbits,
    <<A:8, B:8, C:8, D:8>> = <<M:Maskbits, 0:Rem>>,
    {{A, B, C, D}, Maskbits};
anchor_mask(Addr={_, _, _, _, _, _, _, _}, Maskbits) ->
    M = mask_address(Addr, Maskbits),
    Rem = 128 - Maskbits,
    <<A:16, B:16, C:16, D:16, E:16, F:16, G:16, H:16>> = <<M:Maskbits, 0:Rem>>,
    {{A, B, C, D, E, F, G, H}, Maskbits}.

anchor_mask({Addr, Mask}) ->
    anchor_mask(Addr, Mask).

prettyprint_cidr({Addr, Mask}) ->
    io_lib:format("~s/~B", [inet_parse:ntoa(Addr), Mask]).



validate_options(Options) ->
    %% Check if password is an option
    case lookup("password", Options) of
        undefined ->
            validate_groups_option(Options);
        Pass ->
            case validate_password_option(Pass, Options) of
                {ok, NewOptions} ->
                    validate_groups_option(NewOptions);
                Error ->
                    Error
            end
    end.

validate_groups_option(Options) ->
    case lookup("groups", Options) of
        undefined ->
            {ok, Options};
        GroupStr ->
            Groups= [list_to_binary(G) || G <-
                                         string:tokens(GroupStr, ",")],

            case unknown_roles(Groups, group) of
                [] ->
                    {ok, stash("groups", {"groups", Groups},
                               Options)};
                UnknownGroups ->
                    {error, {unknown_groups, UnknownGroups}}
            end
    end.

%% Handle 'password' option if given
validate_password_option(Pass, Options) ->
    case riak_core_pw_auth:hash_password(list_to_binary(Pass)) of
        {ok, HashedPass, AuthName, HashFunction, Salt, Iterations} ->
            %% Add to options, replacing plaintext password
            NewOptions = stash("password", {"password",
                                            [{hash_pass, HashedPass},
                                             {auth_name, AuthName},
                                             {hash_func, HashFunction},
                                             {salt, Salt},
                                             {iterations, Iterations}]},
                               Options),
            {ok, NewOptions};
        {error, Error} ->
            {error, Error}
    end.


validate_permissions(Perms) ->
    KnownPermissions = app_helper:get_env(riak_core, permissions, []),
    validate_permissions(Perms, KnownPermissions).

validate_permissions([], _) ->
    ok;
validate_permissions([Perm|T], Known) ->
    case string:tokens(Perm, ".") of
        [App, P] ->
            try {list_to_existing_atom(App), list_to_existing_atom(P)} of
                {AppAtom, PAtom} ->
                    case lists:member(PAtom, lookup(AppAtom, Known, [])) of
                        true ->
                            validate_permissions(T, Known);
                        false ->
                            {error, {unknown_permission, Perm}}
                    end
            catch
                error:badarg ->
                            {error, {unknown_permission, Perm}}
            end;
        _ ->
            {error, {unknown_permission, Perm}}
    end.

match_source([], _User, _PeerIP) ->
    {error, no_matching_sources};
match_source([{UserName, {IP,Mask}, Source, Options}|Tail], User, PeerIP) ->
    case (UserName == all orelse
          UserName == User) andalso
        mask_address(IP, Mask) == mask_address(PeerIP, Mask) of
        true ->
            {ok, Source, Options};
        false ->
            match_source(Tail, User, PeerIP)
    end.

sort_sources(Sources) ->
    %% sort sources first by userlist, so that 'all' matches come last
    %% and then by CIDR, so that most sprcific masks come first
    Sources1 = lists:sort(fun({UserA, _, _, _}, {UserB, _, _, _}) ->
                    case {UserA, UserB} of
                        {all, all} ->
                            true;
                        {all, _} ->
                            %% anything is greater than 'all'
                            true;
                        {_, all} ->
                            false;
                        {_, _} ->
                            true
                    end
            end, Sources),
    lists:sort(fun({_, {_, MaskA}, _, _}, {_, {_, MaskB}, _, _}) ->
                MaskA > MaskB
        end, Sources1).

%% group users sharing the same CIDR/Source/Options
group_sources(Sources) ->
    D = lists:foldl(fun({User, CIDR, Source, Options}, Acc) ->
                dict:append({CIDR, Source, Options}, User, Acc)
        end, dict:new(), Sources),
    R1 = [{Users, CIDR, Source, Options} || {{CIDR, Source, Options}, Users} <-
                                       dict:to_list(D)],
    %% Split any entries where the user list contains 'all' so that 'all' has
    %% its own entry. We could actually elide any user sources that overlap
    %% with an 'all' source, but that may be more confusing because deleting
    %% the all source would then 'ressurrect' the user sources.
    R2 = lists:foldl(fun({Users, CIDR, Source, Options}=E, Acc) ->
                    case lists:member(all, Users) of
                        true ->
                            [{[all], CIDR, Source, Options},
                             {Users -- [all], CIDR, Source, Options}|Acc];
                        false ->
                            [E|Acc]
                    end
            end, [], R1),
    %% sort the result by the same criteria that sort_sources uses
    R3 = lists:sort(fun({UserA, _, _, _}, {UserB, _, _, _}) ->
                    case {UserA, UserB} of
                        {[all], [all]} ->
                            true;
                        {[all], _} ->
                            %% anything is greater than 'all'
                            true;
                        {_, [all]} ->
                            false;
                        {_, _} ->
                            true
                    end
            end, R2),
    lists:sort(fun({_, {_, MaskA}, _, _}, {_, {_, MaskB}, _, _}) ->
                MaskA > MaskB
        end, R3).

group_grants(Grants) ->
    D = lists:foldl(fun({{_Role, Bucket}, G}, Acc) ->
                dict:append(Bucket, G, Acc)
        end, dict:new(), Grants),
    [{Bucket, lists:usort(flatten_once(P))} || {Bucket, P} <- dict:to_list(D)].

flatten_once(List) ->
    lists:foldl(fun(A, Acc) ->
                        A ++ Acc
                end, [], List).

%% XXX - not yet invoked because we haven't defined del_group/1
delete_group_from_roles(Groupname) ->
    %% delete the group out of any user or group's 'roles' option
    %% this is kind of a pain, as we have to iterate ALL roles
    delete_group_from_roles(Groupname, <<"users">>),
    delete_group_from_roles(Groupname, <<"groups">>).

delete_group_from_roles(Groupname, RoleType) ->
    riak_core_metadata:fold(fun({_, [?TOMBSTONE]}, Acc) ->
                                    Acc;
                               ({Rolename, [Options]}, Acc) ->
                                    case proplists:get_value("groups", Options) of
                                        undefined ->
                                            Acc;
                                        Groups ->
                                            case lists:member(Groupname,
                                                              Groups) of
                                                true ->
                                                    NewGroups = lists:keystore("groups", 1, Options, {"groups", Groups -- [Groupname]}),
                                                    riak_core_metadata:put({<<"security">>,
                                                                            RoleType},
                                                                           Rolename,
                                                                           NewGroups),
                                                    Acc;
                                                false ->
                                                    Acc
                                            end
                                    end
                            end, undefined,
                            {<<"security">>,RoleType}).


delete_user_from_sources(Username) ->
    riak_core_metadata:fold(fun({{User, _CIDR}=Key, _}, Acc)
                                  when User == Username ->
                                    riak_core_metadata:delete({<<"security">>,
                                                               <<"sources">>},
                                                              Key),
                                    Acc;
                               ({{_, _}, _}, Acc) ->
                                    Acc
                            end, [], {<<"security">>, <<"sources">>}).

%%%% Role identification functions

%% Take a list of roles (users & groups) and return any that can't
%% be found.
unknown_roles(RoleList) ->
    unknown_roles(unknown_roles(RoleList, user), group).

unknown_roles(RoleList, user) ->
    unknown_roles(RoleList, <<"users">>);
unknown_roles(RoleList, group) ->
    unknown_roles(RoleList, <<"groups">>);
unknown_roles(RoleList, RoleType) ->
    riak_core_metadata:fold(fun({Rolename, _}, Acc) ->
                                    Acc -- [Rolename]
                            end, RoleList, {<<"security">>,
                                            RoleType}).

user_details(U) ->
    role_details(U, user).

group_details(G) ->
    role_details(G, group).

%% When we don't know whether a role name is a group or a user, use this
role_details(Rolename) ->
    case role_details(Rolename, user) of
        undefined ->
            role_details(Rolename, group);
        Details ->
            Details
    end.

role_details(Rolename, user) ->
    role_details(Rolename, <<"users">>);
role_details(Rolename, group) ->
    role_details(Rolename, <<"groups">>);
role_details(Rolename, RoleType) ->
    riak_core_metadata:get({<<"security">>, RoleType}, Rolename).

user_exists(Username) ->
    role_exists(Username, user).

group_exists(Groupname) ->
    role_exists(Groupname, group).

role_exists(Rolename, user) ->
    role_exists(Rolename, <<"users">>);
role_exists(Rolename, group) ->
    role_exists(Rolename, <<"groups">>);
role_exists(Rolename, RoleType) ->
    case role_details(Rolename, RoleType) of
        undefined ->
            false;
        _ -> true
    end.

