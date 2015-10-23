-module(riak_core_security_cli).

-behaviour(clique_handler).
-export([
         register_cli/0,
         print_users/3, print_user/3,
         print_groups/3, print_group/3,
         print_sources/3, print_grants/3,
         add_user/3, alter_user/3, del_user/3,
         add_group/3, alter_group/3, del_group/3,
         security_status/3, security_enable/3, security_disable/3
        ]).

-export([
         add_source/1, del_source/1, grant/1, revoke/1,
         ciphers/1
        ]).

-spec register_cli() -> ok.
register_cli() ->
    register_cli_usage(),
    register_cli_cmds().

register_cli_usage() ->
    clique:register_usage(["riak-admin", "security"], base_usage()),
    clique:register_usage(["riak-admin", "security", "print-users"], print_users_usage()),
    clique:register_usage(["riak-admin", "security", "print-user"], print_user_usage()),
    clique:register_usage(["riak-admin", "security", "print-groups"], print_groups_usage()),
    clique:register_usage(["riak-admin", "security", "print-group"], print_group_usage()),
    clique:register_usage(["riak-admin", "security", "print-sources"], print_sources_usage()),
    clique:register_usage(["riak-admin", "security", "print-grants"], print_grants_usage()),
    clique:register_usage(["riak-admin", "security", "add-user"], add_user_usage()),
    clique:register_usage(["riak-admin", "security", "alter-user"], alter_user_usage()),
    clique:register_usage(["riak-admin", "security", "del-user"], del_user_usage()),
    clique:register_usage(["riak-admin", "security", "add-group"], add_group_usage()),
    clique:register_usage(["riak-admin", "security", "alter-group"], alter_group_usage()),
    clique:register_usage(["riak-admin", "security", "del-group"], del_group_usage()),
    clique:register_usage(["riak-admin", "security", "status"], status_usage()),
    clique:register_usage(["riak-admin", "security", "enable"], enable_usage()),
    clique:register_usage(["riak-admin", "security", "disable"], disable_usage()).


register_cli_cmds() ->
    lists:foreach(fun(Args) -> apply(clique, register_command, Args) end,
                  [print_users_register(), print_user_register(),
                   print_groups_register(), print_group_register(),
                   print_grants_register(), print_sources_register(),
                   add_user_register(), alter_user_register(), del_user_register(),
                   add_group_register(), alter_group_register(), del_group_register(),
                   status_register(), enable_register(), disable_register() ]).

%%%
%% Usage
%%%
base_usage() ->
    "riak-admin security <command>\n\n"
    "The following commands modify users and security ACLs for Riak:\n\n"
    "    add-user <username> [<option>=<value> [...]]\n"
    "    add-group <groupname> [<option>=<value> [...]]\n"
    "    alter-user <username> <option> [<option>=<value> [...]]\n"
    "    alter-group <groupname> <option> [<option>=<value> [...]]\n"
    "    del-user <username>\n"
    "    del-group <groupname>\n"
    "    add-source all|<users> <CIDR> <source> [<option>=<value> [...]]\n"
    "    del-source all|<users> <CIDR>\n"
    "    grant <permissions> on any|<type> [bucket] to <users>\n"
    "    revoke <permissions> on any|<type> [bucket] from <users>\n"
    "    print-users\n"
    "    print-groups\n"
    "    print-user <user>\n"
    "    print-group <group>\n"
    "    print-grants <user|group>\n"
    "    print-sources\n"
    "    enable\n"
    "    disable\n"
    "    status\n"
    "    ciphers [cipherlist]\n".

% Obvious usage is used obviously.
status_usage() ->
    "riak-admin security status\n"
    "    Show the status of the cluster security.\n".

enable_usage() ->
    "riak-admin security enable\n"
    "    Enable security.\n".

disable_usage() ->
    "riak-admin security enable\n"
    "    Disable security.\n".

print_users_usage() ->
    "riak-admin security print-users\n"
    "    Print all users.\n".

print_user_usage() ->
    "riak-admin security print-user <user>\n"
    "    Print a single user.\n".

print_groups_usage() ->
    "riak-admin security print-groups\n"
    "    Print all groups.\n".

print_group_usage() ->
    "riak-admin security print-group <group>\n"
    "    Print a single group.\n".

print_sources_usage() ->
    "riak-admin security print-sources\n"
    "    Print all sources.\n".

print_grants_usage() ->
    "riak-admin security print-grants\n"
    "    Print all grants.\n".

add_user_usage() ->
    "riak-admin security add-user <user> [<option>=<value> [...]]\n"
    "    Add a user called <user>.\n".

alter_user_usage() ->
    "riak-admin security alter-user <user> [<option>=<value> [...]]\n"
    "    Alter a user called <user>.\n".

del_user_usage() ->
    "riak-admin security del-user <user>\n"
    "    Delete a user called <user>.\n".

add_group_usage() ->
    "riak-admin security add-group <group> [<option>=<value> [...]]\n"
    "    Add a group called <group>.\n".

alter_group_usage() ->
    "riak-admin security alter-group <group> [<option>=<value> [...]]\n"
    "    Alter a group called <group>.\n".

del_group_usage() ->
    "riak-admin security del-group <group>\n"
    "    Delete a group called <group>.\n".

%%%
%% Registration
%%%

status_register() ->
    [["riak-admin", "security", "status"],
     [],
     [],
     fun security_status/3].

enable_register() ->
    [["riak-admin", "security", "enable"],
     [],
     [],
     fun security_enable/3].

disable_register() ->
    [["riak-admin", "security", "disable"],
     [],
     [],
     fun security_disable/3].

print_users_register() ->
    [["riak-admin", "security", "print-users"],
     [],
     [],
     fun print_users/3].

print_user_register() ->
    [["riak-admin", "security", "print-user", '*'],
     [],
     [],
     fun print_user/3].

print_groups_register() ->
    [["riak-admin", "security", "print-groups"],
     [],
     [],
     fun print_groups/3].

print_group_register() ->
    [["riak-admin", "security", "print-group", '*'],
     [],
     [],
     fun print_group/3].

print_sources_register() ->
    [["riak-admin", "security", "print-sources"],
     [],
     [],
     fun print_sources/3].

print_grants_register() ->
    [["riak-admin", "security", "print-grants", '*'],
     [],
     [],
     fun print_grants/3].

add_user_register() ->
    % "    add-user <username> [<option>=<value> [...]]\n"
    GroupsArg = {groups, [{longname, "groups"}]},
    PasswordArg = {password, [{longname, "password"}]},
    [["riak-admin", "security", "add-user", '*'], %% Cmd
     [GroupsArg, PasswordArg], %% KeySpecs
     [], %% FlagSpecs
     fun(C, O, F) -> add_user(C, atom_keys_to_strings(O), F) end]. %% Callback

alter_user_register() ->
    % "    alter-user <username> [<option>=<value> [...]]\n"
    GroupsArg = {groups, [{longname, "groups"}]},
    PasswordArg = {password, [{longname, "password"}]},
    [["riak-admin", "security", "alter-user", '*'], %% Cmd
     [GroupsArg, PasswordArg], %% KeySpecs
     [], %% FlagSpecs
     fun(C, O, F) -> alter_user(C, atom_keys_to_strings(O), F) end]. %% Callback

del_user_register() ->
    % "    del-user <username>\n"
    [["riak-admin", "security", "del-user", '*'], %% Cmd
     [],
     [],
     fun del_user/3 ].  %% Callback

add_group_register() ->
    % "    add-group <groupname> [<option>=<value> [...]]\n"
    GroupsArg = {groups, [{longname, "groups"}]},
    [["riak-admin", "security", "add-group", '*'], %% Cmd
     [GroupsArg], %% KeySpecs TODO
     [], %% FlagSpecs TODO
     fun(C, O, F) -> add_group(C, atom_keys_to_strings(O), F) end]. %% Callback

alter_group_register() ->
    % "    alter-group <groupname> [<option>=<value> [...]]\n"
    GroupsArg = {groups, [{longname, "groups"}]},
    [["riak-admin", "security", "alter-group", '*'], %% Cmd
     [GroupsArg], %% KeySpecs TODO
     [], %% FlagSpecs TODO
     fun(C, O, F) -> alter_group(C, atom_keys_to_strings(O), F) end]. %% Callback

del_group_register() ->
    % "    del-group <groupname>\n"
    [["riak-admin", "security", "del-group", '*'], %% Cmd
     [],
     [],
     fun del_group/3 ].  %% Callback

atom_keys_to_strings(Opts) ->
    [ {atom_to_list(Key), Val} || {Key, Val} <- Opts ].

%%%
%% Handlers
%%%

security_enable(_Cmd, [], []) ->
    riak_core_security:enable(),
    security_status(_Cmd, [], []).

security_disable(_Cmd, [], []) ->
    riak_core_security:disable(),
    security_status(_Cmd, [], []).

security_status(_Cmd, [], []) ->
    case riak_core_security:status() of
        enabled ->
            [clique_status:text("Enabled\n")];
        disabled ->
            [clique_status:text("Disabled\n")];
        enabled_but_no_capability ->
            [clique_status:text("WARNING: Configured to be enabled, but not supported "
                      "on all nodes so it is disabled!\n")]
    end.

print_user(["riak-admin", "security", "print-user", User], [], []) ->
    case riak_core_security:format_user(User) of
        {error, _}=Error ->
            Output = [clique_status:text(security_error_xlate(Error))],
            %% TODO Maybe we should use an exit_status here
            [clique_status:alert(Output)];
        [_|_]=Users -> % NB No [] match as that's an {error, ...}
            [clique_status:table(Users)]
    end.

print_users(_Cmd, [], []) ->
    case riak_core_security:format_users() of
        [] -> [];
        [_|_]=Users ->
            [clique_status:table(Users)]
    end.

print_group(["riak-admin", "security", "print-group", Group], [], []) ->
    case riak_core_security:format_group(Group) of
        {error, _}=Error ->
            fmt_error(Error);
        [_|_]=Groups ->
            [clique_status:table(Groups)]
    end.

print_groups(["riak-admin", "security", "print-groups"], [], []) ->
    case riak_core_security:format_groups() of
        [] -> [];
        [_|_]=Groups ->
            [clique_status:table(Groups)]
    end.

print_sources(["riak-admin", "security", "print-sources"], [], []) ->
    case riak_core_security:format_sources() of
        [] -> [];
        [_|_]=Sources ->
            [clique_status:table(Sources)]
    end.

print_grants(["riak-admin", "security", "print-grants", Name], [], []) ->
    case riak_core_security:format_grants(Name) of
        {error,_}=Error ->
            fmt_error(Error);
        [_|_]=OK ->
            lists:flatten(
              [[ clique_status:text(Hdr), clique_status:table(Tbl) ]
               || {Hdr, [_|_]=Tbl} <- OK ])
    end.


add_group(["riak-admin", "security", "add-group", Groupname], Options, []) ->
    add_role(Groupname, Options, fun riak_core_security:add_group/2).

alter_group(["riak-admin", "security", "alter-group", Groupname], Options, []) ->
    alter_role(Groupname, Options, fun riak_core_security:alter_group/2).

del_group(["riak-admin", "security", "del-group", Groupname], [], []) ->
    del_role(Groupname, fun riak_core_security:del_group/1).

add_user(["riak-admin", "security", "add-user", Username], Options, []) ->
    add_role(Username, Options, fun riak_core_security:add_user/2).

alter_user(["riak-admin", "security", "alter-user", Username], Options, []) ->
    alter_role(Username, Options, fun riak_core_security:alter_user/2).

del_user(["riak-admin", "security", "del-user", Username], [], []) ->
    del_role(Username, fun riak_core_security:del_user/1).

add_role(Name, Options, Fun) ->
    try Fun(Name, Options) of
        ok ->
            []; %% TODO This can't be the desired outcome
        {error,_}=Error ->
            fmt_error(Error)
    catch % TODO This catch can go now that we're using clique.
        throw:{error, {invalid_option, Option}} ->
            Msg = io_lib:format("Invalid option ~p, options are of the form key=value~n",
                      [Option]),
            [clique_status:alert([clique_status:text(Msg)])]
    end.

alter_role(Name, Options, Fun) ->
    try Fun(Name, Options) of
        ok ->
            [];
        {error,_}=Error ->
            fmt_error(Error)
    catch
        throw:{error, {invalid_option, Option}} ->
            Msg = io_lib:format("Invalid option ~p, options are of the form key=value~n",
                      [Option]),
            [clique_status:alert([clique_status:text(Msg)])]
    end.

del_role(Name, Fun) ->
    case Fun(Name) of
        ok -> [];
        {error,_}=Error ->
            fmt_error(Error)
    end.

fmt_error({error, _Reason}=Err) ->
    Output = [clique_status:text(security_error_xlate(Err))],
    [clique_status:alert(Output)].

%%%
%%% Here be dragons.
%%%

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
security_error_xlate({error, no_matching_ciphers}) ->
    "No known or supported ciphers in list";

%% If we get something we hadn't planned on, better an ugly error
%% message than an ugly RPC call failure
security_error_xlate(Error) ->
    io_lib:format("~p", [Error]).

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

ciphers([]) ->
    riak_core_security:print_ciphers();

ciphers([CipherList]) ->
    case riak_core_security:set_ciphers(CipherList) of
        ok ->
            riak_core_security:print_ciphers(),
            ok;
        {error, _} = Error ->
            io:format(security_error_xlate(Error)),
            io:format("~n"),
            Error
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
