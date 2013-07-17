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

%% TODO
-compile(export_all).


initial_config() ->
    %[{users, [{"andrew", [{password, "foo"}]}]},
    [{users, []},
     {sources, []},
     {grants, []},
     {revokes, []},
     {epoch, os:timestamp()}
    ].

get_meta() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    case riak_core_ring:get_meta(?MODULE, Ring) of
        undefined ->
            initial_config();
        {ok, Meta} ->
            Meta
    end.

put_meta(Meta) ->
    riak_core_ring_manager:ring_trans(fun(Ring, M) ->
                {new_ring, riak_core_ring:update_meta(?MODULE, M, Ring)}
        end, Meta).

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

prettyprint_users([all]) ->
    "all";
prettyprint_users(Users) ->
    string:join(Users, ", ").

match_source([], _User, _PeerIP) ->
    {error, no_matching_sources};
match_source([{{UserName, {IP,Mask}}, Source, Options}|Tail], User, PeerIP) ->
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
    Sources1 = lists:sort(fun({{UserA, _}, _, _}, {{UserB, _}, _, _}) ->
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
    lists:sort(fun({{_, {_, MaskA}}, _, _}, {{_, {_, MaskB}}, _, _}) ->
                MaskA > MaskB
        end, Sources1).

%% group users sharing the same CIDR/Source/Options
group_sources(Sources) ->
    D = lists:foldl(fun({{User, CIDR}, Source, Options}, Acc) ->
                dict:append({CIDR, Source, Options}, User, Acc)
        end, dict:new(), Sources),
    R1 = [{Users, CIDR, Source, Options} || {{CIDR, Source, Options}, Users} <-
                                       dict:to_list(D)],
    %% sort the result by the same criteria that sort_sources uses
    R2 = lists:sort(fun({UserA, _, _, _}, {UserB, _, _, _}) ->
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
            end, R1),
    lists:sort(fun({_, {_, MaskA}, _, _}, {_, {_, MaskB}, _, _}) ->
                MaskA > MaskB
        end, R2).

print_sources() ->
    Meta = get_meta(),
    Sources = lookup(sources, Meta, []),
    print_sources(Sources).

print_sources(Sources) ->
    GS = group_sources(Sources),
    table:print([{users, 20}, {cidr, 10}, {source, 10}, {options, 10}],
                [[prettyprint_users(Users), prettyprint_cidr(CIDR),
                  atom_to_list(Source), io_lib:format("~p", [Options])] ||
            {Users, CIDR, Source, Options} <- GS]).

print_users() ->
    Meta = get_meta(),
    Users = lookup(users, Meta, []),
    table:print([{username, 20}, {options, 10}],
                [[Username, io_lib:format("~p", [Options])] ||
            {Username, Options} <- Users]).

%% Contexts are only valid until the GRANT epoch changes, and it will change
%% whenever a GRANT or a REVOKE is performed. This is a little coarse grained
%% right now, but it'll do for the moment.
get_context(Username, Meta) ->
    Grants = lookup(Username, lookup(grants, Meta, []), []),
    Revokes = lookup(Username, lookup(revokes, Meta, []), []),
    {Username, Grants, Revokes, lookup(epoch, Meta)}.

check_permission(Permission, Bucket, Context) ->
    Meta = get_meta(),
    Epoch = lookup(epoch, Meta),
    {Username, Grants, Revokes, CtxEpoch} = Context,
    case Epoch == CtxEpoch of
        false ->
            %% context has expired
            check_permission(Permission, Bucket, get_context(Username, Meta));
        true ->
            %% TODO check context is still valid
            case match_grant(Bucket, Grants) of
                {B, MatchG} ->
                    case MatchG /= undefined andalso
                        (lists:member(Permission, MatchG) orelse MatchG == 'all') of
                        true ->
                            %% ok, permission is present, check for revokes
                            MatchR = lookup(B, Revokes, undefined),
                            case MatchR /= undefined andalso
                                (lists:member(Permission, MatchR) orelse MatchR == 'all') of
                                true ->
                                    %% oh snap, it was revoked
                                    {false, Context};
                                false ->
                                    %% not revoked, yay
                                    {true, Context}
                            end;
                        false ->
                            %% no applicable grant
                            {false, Context}
                    end;
                undefined ->
                    %% no grants for this user at all
                    {false, Context}
            end
    end.

get_username({Username, _Grants, _Revokes, _Expiry}) ->
    Username.

match_grant(Bucket, Grants) ->
    %% find the first grant that matches the bucket name
    lists:foldl(fun({B, P}, undefined) ->
                case lists:last(B) == $* of
                    true ->
                        L = length(B) - 1,
                        case string:substr(Bucket, 1, L) ==
                             string:substr(B, 1, L) of
                            true ->
                                {B, P};
                            false ->
                                undefined
                        end;
                    false ->
                        case Bucket == B of
                            true ->
                                {B, P};
                            false ->
                                undefined
                        end
                end;
            (_, Acc) ->
                Acc
        end, undefined, lists:sort(fun({A, _}, {B, _}) ->
                    %% sort by descending bucket length, so more specific
                    %% matches come first
                    length(A) > length(B)
            end, Grants)).

authenticate(Username, Password, ConnInfo) ->
    Meta = get_meta(),
    Users = lookup(users, Meta, []),
    case lookup(Username, Users) of
        undefined ->
            {error, unknown_user};
        UserData ->
            Sources = sort_sources(lookup(sources, Meta, [])),
            case match_source(Sources, Username,
                              proplists:get_value(ip, ConnInfo)) of
                {ok, Source, _Options} ->
                    case Source of
                        trust ->
                            %% trust always authenticates
                            {ok, get_context(Username, Meta)};
                        password ->
                            %% pull the password out of the userdata
                            case lookup("password", UserData) of
                                undefined ->
                                    lager:warning("User ~p is configured for "
                                                  "password authentication, but has "
                                                  "no password", [Username]),
                                    {error, missing_password};
                                Pass ->
                                    %% This should be bcrypted or something...
                                    case Password == Pass of
                                        true ->
                                            {ok, get_context(Username, Meta)};
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
                                    case CN == Username of
                                        true ->
                                            {ok, get_context(Username, Meta)};
                                        false ->
                                            {error, common_name_mismatch}
                                    end
                            end;
                        Source ->
                            lager:warning("User ~p is configured with unknown "
                                          "authentication source ~p",
                                          [Username, Source]),
                            {error, unknown_source}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end
    end.

add_user(Username, Options) ->
    Meta = get_meta(),
    Users = lookup(users, Meta, []),
    case lookup(Username, Users) of
        undefined ->
            case validate_options(Options) of
                {ok, NewOptions} ->
                    put_meta(stash(users, {users, stash(Username,
                                                        {Username, NewOptions},
                                                        Users)},
                                   Meta)),
                    ok;
                Error ->
                    Error
            end;
        _ ->
            {error, user_exists}
    end.

add_grant(all, Bucket, Grants) ->
    %% all is always valid
    Meta = get_meta(),
    case validate_permissions(Grants) of
        ok ->
            put_meta(stash(epoch, {epoch, os:timestamp()},
                           add_grant_int([all], Bucket, Grants, Meta)));
        Error ->
            Error
    end;
add_grant([H|_T]=UserList, Bucket, Grants) when is_list(H) ->
    %% list of lists, weeeee
    %% validate the users...
    Meta = get_meta(),
    Users = lookup(users, Meta, []),
    Valid = lists:foldl(fun(User, ok) ->
                    case lists:keymember(User, 1, Users) of
                        true ->
                            ok;
                        false ->
                            {error, {unknown_user, User}}
                    end;
                (_User, Acc) ->
                    Acc
            end, ok, UserList),
    Valid2 = case Valid of
        ok ->
            validate_permissions(Grants);
        Other ->
            Other
    end,
    case Valid2 of
        ok ->
            %% add a source for each user
            put_meta(stash(epoch, {epoch, os:timestamp()},
                           add_grant_int(UserList, Bucket, Grants, Meta))),
            ok;
        Error ->
            Error
    end;
add_grant(User, Bucket, Grants) ->
    %% single user
    add_grant([User], Bucket, Grants).

add_grant_int([], _, _, Meta) ->
    Meta;
add_grant_int([User|Users], Bucket, Permissions, Meta) ->
    Grants = lookup(grants, Meta, []),
    UserGrants = lookup(User, Grants, []),
    BucketPermissions = lookup(Bucket, UserGrants, []),
    NewPerms = lists:umerge(lists:sort(BucketPermissions),
                            lists:sort(Permissions)),
    NewMeta = stash(grants, {grants, stash(User, {User, stash(Bucket, {Bucket,
                                                                      NewPerms},
                                                             UserGrants)},
                                           Grants)}, Meta),
    add_grant_int(Users, Bucket, Permissions, NewMeta).

add_revoke(all, Bucket, Revokes) ->
    %% all is always valid
    Meta = get_meta(),
    case validate_permissions(Revokes) of
        ok ->
            case add_revoke_int([all], Bucket, revokes, Meta) of
                {ok, NewMeta} ->
                    put_meta(stash(epoch, {epoch, os:timestamp()}, NewMeta)),
                    ok;
                Error2 ->
                    Error2
            end;
        Error ->
            Error
    end;
add_revoke([H|_T]=UserList, Bucket, Revokes) when is_list(H) ->
    %% list of lists, weeeee
    %% validate the users...
    Meta = get_meta(),
    Users = lookup(users, Meta, []),
    Valid = lists:foldl(fun(User, ok) ->
                    case lists:keymember(User, 1, Users) of
                        true ->
                            ok;
                        false ->
                            {error, {unknown_user, User}}
                    end;
                (_User, Acc) ->
                    Acc
            end, ok, UserList),
    Valid2 = case Valid of
        ok ->
            validate_permissions(Revokes);
        Other ->
            Other
    end,
    case Valid2 of
        ok ->
            %% add a source for each user
            case add_revoke_int(UserList, Bucket, Revokes, Meta) of
                {ok, NewMeta} ->
                    put_meta(stash(epoch, {epoch, os:timestamp()}, NewMeta)),
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

add_revoke_int([], _, _, Meta) ->
    {ok, Meta};
add_revoke_int([User|Users], Bucket, Permissions, Meta) ->
    Revokes = lookup(revokes, Meta, []),
    UserRevokes = lookup(User, Revokes, []),
    Grants = lookup(grants, Meta, []),
    UserGrants = lookup(User, Grants, []),

    %% check if there is currently a GRANT we can revoke
    case lookup(Bucket, UserGrants) of
        undefined ->
            %% can't REVOKE what wasn't GRANTED
            add_revoke_int(Users, Bucket, Permissions, Meta);
        GrantedPermissions ->
            ToRevoke = [X || X <- Permissions, Y <- GrantedPermissions, X == Y],
            RevokedPermissions = lookup(Bucket, UserRevokes, []),
            NewRevokes = lists:umerge(lists:sort(ToRevoke),
                                      lists:sort(RevokedPermissions)),

            %% Now, in an ideal world, we'd try to do a strongly-consistent
            %% write to simply update the GRANT list, andf if that failed,
            %% we'd write to the REVOKE list. For testing purposes, we'll just
            %% write a REVOKE for now.
            NewMeta = stash(revokes, {revokes, stash(User, {User, stash(Bucket, {Bucket,
                                                                                 NewRevokes},
                                                                        UserRevokes)},
                                                     Revokes)}, Meta),
            add_revoke_int(Users, Bucket, Permissions, NewMeta)
    end.

add_source(all, CIDR, Source, Options) ->
    %% all is always valid
    Meta = get_meta(),
    put_meta(add_source_int([all], anchor_mask(CIDR), Source, Options, Meta)),
    ok;
add_source([H|_T]=UserList, CIDR, Source, Options) when is_list(H) ->
    %% list of lists, weeeee
    %% validate the users...
    Meta = get_meta(),
    Users = lookup(users, Meta, []),
    Valid = lists:foldl(fun(User, ok) ->
                    case lists:keymember(User, 1, Users) of
                        true ->
                            ok;
                        false ->
                            {error, {unknown_user, User}}
                    end;
                (_User, Acc) ->
                    Acc
            end, ok, UserList),
    case Valid of
        ok ->
            %% add a source for each user
            put_meta(add_source_int(UserList, anchor_mask(CIDR), Source,
                                    Options, Meta)),
            ok;
        Error ->
            Error
    end;
add_source(User, CIDR, Source, Options) ->
    %% single user
    add_source([User], CIDR, Source, Options).

add_source_int([], _, _, _, Meta) ->
    Meta;
add_source_int([User|Users], CIDR, Source, Options, Meta) ->
    Sources = lookup(sources, Meta, []),
    NewMeta = stash(sources, {sources, stash({User, CIDR}, {{User, CIDR},
                                                            Source, Options},
                                             Sources)}, Meta),
    add_source_int(Users, CIDR, Source, Options, NewMeta).

rm_source(all, CIDR) ->
    Meta = get_meta(),
    put_meta(rm_source_int(all, anchor_mask(CIDR), Meta)),
    ok;
rm_source([H|_]=UserList, CIDR) when is_list(H) ->
    Meta = get_meta(),
    put_meta(rm_source_int(UserList, anchor_mask(CIDR), Meta)),
    ok;
rm_source(User, CIDR) ->
    %% single user
    rm_source([User], CIDR).

rm_source_int([], _, Meta) ->
    Meta;
rm_source_int([User|Users], CIDR, Meta) ->
    Sources = lookup(sources, Meta, []),
    NewMeta = stash(sources, {sources, lists:keydelete({User, CIDR}, 1,
                                                       Sources)}, Meta),
    rm_source_int(Users, CIDR, NewMeta).


%% XXX this is a stub for now, should tie in JoeD's stuff for validation
validate_options(Options) ->
    %% Check if password is an option
    case lookup("password", Options) of
        undefined ->
            {ok, Options};
        Pass ->
            lager:info("Hashing password: ~p", [Pass]),
            case riak_core_pw_auth:hash_password(Pass) of
                {ok, HashedPass, Algorithm, HashFunction, Salt, Iterations, DerivedLength} ->
                    %% Add to options 
                    NewOptions = stash(hash, {hash, 
                                              [{hash_pass, HashedPass},
                                               {algorithm, Algorithm},
                                               {hash_func, HashFunction},
                                               {salt, Salt},
                                               {iterations, Iterations},
                                               {length, DerivedLength}]},
                                       Options),
                    {ok, NewOptions};
                {error, Error} ->
                    {error, Error}
            end
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

