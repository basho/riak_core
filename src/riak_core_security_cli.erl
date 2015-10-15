-module(riak_core_security_cli).
-export([
         add_user/1, alter_user/1, del_user/1,
         add_group/1, alter_group/1, del_group/1,
         add_source/1, del_source/1, grant/1, revoke/1,
         print_users/1, print_user/1, print_sources/1,
         print_groups/1, print_group/1, print_grants/1,
         security_enable/1, security_disable/1, security_status/1, ciphers/1
        ]).

security_error_xlate({errors, Errors}) ->
    string:join(
      lists:map(fun(X) -> security_error_xlate({error, X}) end,
                Errors),
      "~n");
security_error_xlate({error, unknown_user}) ->
    "User not recognized";
security_error_xlate({error, unknown_group}) ->
    "Group not recognized";
security_error_xlate({error, {unknown_permission, Name}}) ->
    io_lib:format("Permission not recognized: ~ts", [Name]);
security_error_xlate({error, {unknown_role, Name}}) ->
    io_lib:format("Name not recognized: ~ts", [Name]);
security_error_xlate({error, {unknown_user, Name}}) ->
    io_lib:format("User not recognized: ~ts", [Name]);
security_error_xlate({error, {unknown_group, Name}}) ->
    io_lib:format("Group not recognized: ~ts", [Name]);
security_error_xlate({error, {unknown_users, Names}}) ->
    io_lib:format("User(s) not recognized: ~ts",
                  [
                   string:join(
                     lists:map(fun(X) -> unicode:characters_to_list(X, utf8) end, Names),
                     ", ")
                  ]);
security_error_xlate({error, {unknown_groups, Names}}) ->
    io_lib:format("Group(s) not recognized: ~ts",
                  [
                   string:join(
                     lists:map(fun(X) -> unicode:characters_to_list(X, utf8) end, Names),
                     ", ")
                  ]);
security_error_xlate({error, {unknown_roles, Names}}) ->
    io_lib:format("Name(s) not recognized: ~ts",
                  [
                   string:join(
                    lists:map(fun(X) -> unicode:characters_to_list(X, utf8) end, Names),
                    ", ")
                  ]);
security_error_xlate({error, {duplicate_roles, Names}}) ->
    io_lib:format("Ambiguous names need to be prefixed with 'user/' or 'group/': ~ts",
                  [
                   string:join(
                     lists:map(fun(X) -> unicode:characters_to_list(X, utf8) end, Names),
                     ", ")
                  ]);
security_error_xlate({error, reserved_name}) ->
    "This name is reserved for system use";
security_error_xlate({error, no_matching_sources}) ->
    "No matching source";
security_error_xlate({error, illegal_name_char}) ->
    "Illegal character(s) in name";
security_error_xlate({error, role_exists}) ->
    "This name is already in use";

%% If we get something we hadn't planned on, better an ugly error
%% message than an ugly RPC call failure
security_error_xlate(Error) ->
    io_lib:format("~p", [Error]).

add_user([Username|Options]) ->
    add_role(Username, Options, fun riak_core_security:add_user/2).

add_group([Groupname|Options]) ->
    add_role(Groupname, Options, fun riak_core_security:add_group/2).

add_role(Name, Options, Fun) ->
    try Fun(Name, parse_options(Options)) of
        ok ->
            ok;
        Error ->
            io:format(security_error_xlate(Error)),
            io:format("~n"),
            Error
    catch
        throw:{error, {invalid_option, Option}} ->
            io:format("Invalid option ~p, options are of the form key=value~n",
                      [Option]),
            error
    end.

alter_user([Username|Options]) ->
    alter_role(Username, Options, fun riak_core_security:alter_user/2).

alter_group([Groupname|Options]) ->
    alter_role(Groupname, Options, fun riak_core_security:alter_group/2).

alter_role(Name, Options, Fun) ->
    try Fun(Name, parse_options(Options)) of
        ok ->
            ok;
        Error ->
            io:format(security_error_xlate(Error)),
            io:format("~n"),
            Error
    catch
        throw:{error, {invalid_option, Option}} ->
            io:format("Invalid option ~p, options are of the form key=value~n",
                      [Option]),
            error
    end.

del_user([Username]) ->
    del_role(Username, fun riak_core_security:del_user/1).

del_group([Groupname]) ->
    del_role(Groupname, fun riak_core_security:del_group/1).

del_role(Name, Fun) ->
    case Fun(Name) of
        ok ->
            io:format("Successfully deleted ~ts~n", [Name]),
            ok;
        Error ->
            io:format(security_error_xlate(Error)),
            io:format("~n"),
            Error
    end.

add_source([Users, CIDR, Source | Options]) ->
    Unames = case string:tokens(Users, ",") of
        ["all"] ->
            all;
        Other ->
            Other
    end,
    %% Unicode note: atoms are constrained to latin1 until R18, so our
    %% sources are as well
    try riak_core_security:add_source(Unames, parse_cidr(CIDR),
                                  list_to_atom(string:to_lower(Source)),
                                  parse_options(Options)) of
        ok ->
            io:format("Successfully added source~n"),
            ok;
        Error ->
            io:format(security_error_xlate(Error)),
            io:format("~n"),
            Error
    catch
        throw:{error, {invalid_option, Option}} ->
            io:format("Invalid option ~p, options are of the form key=value~n",
                      [Option]);
        error:badarg ->
            io:format("Invalid source ~ts, must be latin1, sorry~n",
                      [Source])
    end.

del_source([Users, CIDR]) ->
    Unames = case string:tokens(Users, ",") of
        ["all"] ->
            all;
        Other ->
            Other
    end,
    riak_core_security:del_source(Unames, parse_cidr(CIDR)),
    io:format("Deleted source~n").


parse_roles(Roles) ->
    case string:tokens(Roles, ",") of
        ["all"] ->
            all;
        Other ->
            Other
    end.

parse_grants(Grants) ->
    string:tokens(Grants, ",").

grant_int(Permissions, Bucket, Roles) ->
    case riak_core_security:add_grant(Roles, Bucket, Permissions) of
        ok ->
            io:format("Successfully granted~n"),
            ok;
        Error ->
            io:format(security_error_xlate(Error)),
            io:format("~n"),
            Error
    end.


grant([Grants, "on", "any", "to", Users]) ->
    grant_int(parse_grants(Grants),
              any,
              parse_roles(Users));
grant([Grants, "on", Type, Bucket, "to", Users]) ->
    grant_int(parse_grants(Grants),
              { Type, Bucket },
              parse_roles(Users));
grant([Grants, "on", Type, "to", Users]) ->
    grant_int(parse_grants(Grants),
              Type,
              parse_roles(Users));
grant(_) ->
    io:format("Usage: grant <permissions> on (<type> [bucket]|any) to <users>~n"),
    error.

revoke_int(Permissions, Bucket, Roles) ->
    case riak_core_security:add_revoke(Roles, Bucket, Permissions) of
        ok ->
            io:format("Successfully revoked~n"),
            ok;
        Error ->
            io:format(security_error_xlate(Error)),
            io:format("~n"),
            Error
    end.

revoke([Grants, "on", "any", "from", Users]) ->
    revoke_int(parse_grants(Grants),
               any,
               parse_roles(Users));
revoke([Grants, "on", Type, Bucket, "from", Users]) ->
    revoke_int(parse_grants(Grants),
               { Type, Bucket },
               parse_roles(Users));
revoke([Grants, "on", Type, "from", Users]) ->
    revoke_int(parse_grants(Grants),
               Type,
               parse_roles(Users));
revoke(_) ->
    io:format("Usage: revoke <permissions> on <type> [bucket] from <users>~n"),
    error.

print_grants([Name]) ->
    case riak_core_security:print_grants(Name) of
        ok ->
            ok;
        Error ->
            io:format(security_error_xlate(Error)),
            io:format("~n"),
            Error
    end.

print_users([]) ->
    riak_core_security:print_users().

print_user([User]) ->
    case riak_core_security:print_user(User) of
        ok ->
            ok;
        Error ->
            io:format(security_error_xlate(Error)),
            io:format("~n"),
            Error
    end.


print_groups([]) ->
    riak_core_security:print_groups().

print_group([Group]) ->
    case riak_core_security:print_group(Group) of
        ok ->
            ok;
        Error ->
            io:format(security_error_xlate(Error)),
            io:format("~n"),
            Error
    end.

print_sources([]) ->
    riak_core_security:print_sources().

ciphers([]) ->
    riak_core_security:print_ciphers();

ciphers([CipherList]) ->
    case riak_core_security:set_ciphers(CipherList) of
        ok ->
            riak_core_security:print_ciphers(),
            ok;
        error ->
            error
    end.

security_enable([]) ->
    riak_core_security:enable().

security_disable([]) ->
    riak_core_security:disable().

security_status([]) ->
    case riak_core_security:status() of
        enabled ->
            io:format("Enabled~n");
        disabled ->
            io:format("Disabled~n");
        enabled_but_no_capability ->
            io:format("WARNING: Configured to be enabled, but not supported "
                      "on all nodes so it is disabled!~n")
    end.

parse_options(Options) ->
    parse_options(Options, []).

parse_options([], Acc) ->
    Acc;
parse_options([H|T], Acc) ->
    case re:split(H, "=", [{parts, 2}, {return, list}]) of
        [Key, Value] when is_list(Key), is_list(Value) ->
            parse_options(T, [{string:to_lower(Key), Value}|Acc]);
        _Other ->
            throw({error, {invalid_option, H}})
    end.

-spec parse_cidr(string()) -> {inet:ip_address(), non_neg_integer()}.
parse_cidr(CIDR) ->
    [IP, Mask] = string:tokens(CIDR, "/"),
    {ok, Addr} = inet_parse:address(IP),
    {Addr, list_to_integer(Mask)}.
