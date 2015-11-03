-module(riak_core_security_cli).

-behaviour(clique_handler).
-export([
         register_cli/0,
         print_users/3, print_user/3,
         print_groups/3, print_group/3,
         print_sources/3,
         print_grants/3,
         print_ciphers/3,
         add_user/3, alter_user/3, del_user/3,
         add_group/3, alter_group/3, del_group/3,
         add_source/3, del_source/3,
         grant/3, revoke/3,
         ciphers/3,
         security_status/3, security_enable/3, security_disable/3
        ]).

-define(CLI_PREFIX, ["riak-admin", "security"]).
-define(CLI_PREFIX(Cmd), ["riak-admin", "security" | Cmd]).

-define(groups_arg, {groups, [{longname, "groups"}]}).
-define(password_arg, {password, [{longname, "password"}]}).

-spec register_cli() -> ok.
register_cli() ->
    register_all(?CLI_PREFIX).

%%%
%% Usage
%%%
register_all(Prefix) ->
    %% TODO This is a bit ugly
    BaseHeadline = io_lib:format(
                     "~s <command>\n\n"
                     "The following commands modify users and security ACLs for Riak:\n\n",
                     [string:join(Prefix, " ")]),
    BaseUsage =
    lists:foldl(
      fun({Detail, Usage, [Cmd|_]=Spec, Keys, Flags, Fun}, BaseUsage) ->
              %% TODO So much use of io_lib offends me.
              FullUsage = io_lib:format("~s~n    ~s~n", [Usage, Detail]),
              true = clique:register_usage(Prefix ++ [Cmd], FullUsage),
              %% TODO What if this doesn't return ok ?
              ok = clique:register_command(Prefix ++ Spec, Keys, Flags, Fun),
              io_lib:format("~s    ~s~n", [BaseUsage, Usage])
      end,
      BaseHeadline,
      [
       register_security_status(),
       register_security_enable(),
       register_security_disable(),
       register_print_users(),
       register_print_user(),
       register_print_groups(),
       register_print_group(),
       register_print_sources(),
       register_print_grants(),
       register_print_ciphers(),
       register_add_user(),
       register_alter_user(),
       register_del_user(),
       register_add_group(),
       register_alter_group(),
       register_del_group(),
       register_add_source(),
       register_del_source(),
       register_ciphers(),
       register_grant_type(),
       register_grant_type_bucket(),
       register_revoke_type(),
       register_revoke_type_bucket()]),
    clique:register_usage(Prefix, BaseUsage).

% Obvious usage is used obviously.
register_security_status() ->
    {"Show the status of the cluster security.",
     "status",
     ["status"], [], [], fun security_status/3}.

register_security_enable() ->
    {"Enable security.",
     "enable",
     ["enable"], [], [], fun security_enable/3}.

register_security_disable() ->
    {"Disable security.",
     "disable",
    ["disable"], [], [], fun security_disable/3}.

register_print_users() ->
    {"Print all users.",
     "print-users",
    ["print-users"], [], [], fun print_users/3}.

register_print_user() ->
    {"Print a single user.",
    "print-user <user>",
    ["print-user", '*'], [], [], fun print_user/3}.

register_print_groups() ->
    {"Print all groups.",
     "print-groups",
    ["print-groups"], [], [], fun print_groups/3}.

register_print_group() ->
    {"Print a single group.",
     "print-group <group>",
    ["print-group", '*'], [], [], fun print_group/3}.

register_print_sources() ->
    {"Print all sources.",
     "print-sources",
    ["print-sources"], [], [], fun print_sources/3}.

register_print_grants() ->
    {"Print all grants.",
     "print-grants <identifier>",
    ["print-grants", '*'], [], [], fun print_grants/3}.

register_print_ciphers() ->
    {"Print all configured, valid and invalid ciphers.",
     "print-ciphers",
    ["print-ciphers"], [], [], fun print_ciphers/3}.

register_add_user() ->
    {"Add a user called <user>.",
     "add-user <user> [<option>=<value> [...]]",
    ["add-user", '*'], %% Cmd
    [?groups_arg, ?password_arg], %% KeySpecs
    [], %% FlagSpecs
    fun(C, O, F) -> add_user(C, atom_keys_to_strings(O), F) end}.

register_alter_user() ->
    {"Alter a user called <user>.",
     "alter-user <user> [<option>=<value> [...]]",
    ["alter-user", '*'], %% Cmd
    [?groups_arg, ?password_arg], %% KeySpecs
    [], %% FlagSpecs
    fun(C, O, F) -> alter_user(C, atom_keys_to_strings(O), F) end }.

register_del_user() ->
    {"Delete a user called <user>.",
     "del-user <user>",
    ["del-user", '*'], %% Cmd
    [],
    [],
    fun del_user/3 }.

register_add_group() ->
    {"Add a group called <group>.",
     "add-group <group> [<option>=<value> [...]]",
    ["add-group", '*'], %% Cmd
    [?groups_arg], %% KeySpecs
    [], %% FlagSpecs
    fun(C, O, F) -> add_group(C, atom_keys_to_strings(O), F) end }.

register_alter_group() ->
    {"Alter a group called <group>.",
     "alter-group <group> [<option>=<value> [...]]",
    ["alter-group", '*'], %% Cmd
    [?groups_arg], %% KeySpecs
    [], %% FlagSpecs
    fun(C, O, F) -> alter_group(C, atom_keys_to_strings(O), F) end }.

register_del_group() ->
    {"Delete a group called <group>.",
     "del-group <group>",
    ["del-group", '*'],
    [],
    [],
    fun del_group/3 }.

register_add_source() ->
    {"Add a <source> (e.g. 'password') for <CIDR> to a list of <users> or 'all'.",
     "add-source all|<users> <CIDR> <source> [<option>=<value> [...]]",
    ["add-source", '*', '*', '*'], %% Cmd
    [], %% TODO Some Arg specs definitely. what options are allowed?
    [],
    fun add_source/3 }.

register_del_source() ->
    {"Delete source <CIDR> for 'all' users or only <users>.",
     "del-source all|<users> <CIDR>",
    ["del-source", '*', '*'], %% Cmd,
    [], %% TODO Some Arg specs definitely. what options are allowed?
    [],
    fun del_source/3 }.

register_ciphers() ->
    {"Configure the ciphers available.",
     "ciphers <cipher-list>",
    ["ciphers", '*'],
    [],
    [],
    fun ciphers/3 }.

register_grant_type() ->
    {"Grant <permissions> on specified bucket (or any) to <users>.",
     "grant <permissions> on any|<type> [bucket] to <users>",
    ["grant", '*', '*', '*', '*', '*'],
    [],
    [],
    fun grant/3 }.

%% TODO Is there a way to register this as part of register_grant_type() above?
register_grant_type_bucket() ->
    {"Grant <permissions> on specified bucket (or any) to <users>.",
     "grant <permissions> on any|<type> [bucket] to <users>",
    ["grant", '*', '*', '*', '*', '*', '*'],
    [],
    [],
    fun grant/3 }.

register_revoke_type() ->
    {"Revoke <permissions> from <users> on specified (or any) bucket.",
     "revoke <permissions> on any|<type> [bucket] from <users>",
    ["revoke", '*', '*', '*', '*', '*'],
    [],
    [],
    fun revoke/3 }.

register_revoke_type_bucket() ->
    {"Revoke <permissions> from <users> on specified (or any) bucket.",
     "revoke <permissions> on any|<type> [bucket] from <users>",
    ["revoke", '*', '*', '*', '*', '*', '*'],
    [],
    [],
    fun revoke/3 }.

atom_keys_to_strings(Opts) ->
    [ {atom_to_list(Key), Val} || {Key, Val} <- Opts ].

%%%
%% Handlers
%%%

%% TODO There has to be a nicer pattern to chain a print-* command
%% after a config change
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

maybe_empty_table([]) -> [];
maybe_empty_table({error, _}=Error) -> fmt_error(Error);
maybe_empty_table([_|_]=Rows) -> [clique_status:table(Rows)].

print_user(?CLI_PREFIX(["print-user", User]), [], []) ->
    maybe_empty_table(riak_core_security:format_user(User)).

print_users(_Cmd, [], []) ->
    maybe_empty_table(riak_core_security:format_users()).

print_group(?CLI_PREFIX(["print-group", Group]), [], []) ->
    maybe_empty_table(riak_core_security:format_group(Group)).

print_groups(?CLI_PREFIX(["print-groups"]), [], []) ->
    maybe_empty_table(riak_core_security:format_groups()).

print_sources(?CLI_PREFIX(["print-sources"]), [], []) ->
    maybe_empty_table(riak_core_security:format_sources()).

print_grants(?CLI_PREFIX(["print-grants", Name]), [], []) ->
    %% TODO Maybe this wasn't the best return structure?
    case riak_core_security:format_grants(Name) of
        {error,_}=Error ->
            fmt_error(Error);
        [_|_]=OK ->
            lists:flatten(
              [[ clique_status:text(Hdr), clique_status:table(Tbl) ]
               || {Hdr, [_|_]=Tbl} <- OK ])
    end.

print_ciphers(?CLI_PREFIX(["print-ciphers"]), [], []) ->
    [ clique_status:text(S) || S <- riak_core_security:format_ciphers()].

add_group(?CLI_PREFIX(["add-group", Groupname]), Options, []) ->
    alter_role(Groupname, Options, fun riak_core_security:add_group/2).

alter_group(?CLI_PREFIX(["alter-group", Groupname]), Options, []) ->
    alter_role(Groupname, Options, fun riak_core_security:alter_group/2).

del_group(?CLI_PREFIX(["del-group", Groupname]), [], []) ->
    del_role(Groupname, fun riak_core_security:del_group/1).

add_user(?CLI_PREFIX(["add-user", Username]), Options, []) ->
    alter_role(Username, Options, fun riak_core_security:add_user/2).

alter_user(?CLI_PREFIX(["alter-user", Username]), Options, []) ->
    alter_role(Username, Options, fun riak_core_security:alter_user/2).

del_user(?CLI_PREFIX(["del-user", Username]), [], []) ->
    del_role(Username, fun riak_core_security:del_user/1).

alter_role(Name, Options, Fun) ->
    case Fun(Name, Options) of
        ok -> []; %% TODO Is this really all we need?
        {error,_}=Error ->
            fmt_error(Error)
    end.

del_role(Name, Fun) ->
    case Fun(Name) of
        ok -> []; %% TODO Is this really all we need?
        {error,_}=Error ->
            fmt_error(Error)
    end.

add_source(?CLI_PREFIX(["add-source", Users, CIDR, Source]), Options, []) ->
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
        %% TODO We shouldn't need to use parse_options/1 with clique..?
        %% But maybe it's the only way to enforce the latin1 restriction
        ok ->
            [clique_status:text("Successfully added source")];
        {error,_}=Error ->
            fmt_error(Error)
    catch
        error:badarg ->
            fmt_error({error, badarg})
    end.

del_source(?CLI_PREFIX(["del-source", Users, CIDR]), [], []) ->
    Unames = case string:tokens(Users, ",") of
        ["all"] ->
            all;
        Other ->
            Other
    end,
    %% TODO Should this let you know if nothing was deleted...?
    ok = riak_core_security:del_source(Unames, parse_cidr(CIDR)),
    [clique_status:text("Deleted source~n")].

%%%
%%% Here be dragons.
%%%

fmt_error({error, _Reason}=Err) ->
    Output = [clique_status:text(security_error_xlate(Err))],
    [clique_status:alert(Output)].

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
            [clique_status:text("Successfully granted")];
        {error,_}=Error ->
            fmt_error(Error)
    end.

grant(?CLI_PREFIX(["grant" | Grants]), [], []) ->
    grant(Grants).

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
              parse_roles(Users)).

revoke_int(Permissions, Bucket, Roles) ->
    case riak_core_security:add_revoke(Roles, Bucket, Permissions) of
        ok ->
            [clique_status:text("Successfully revoked")];
        {error,_}=Error ->
            fmt_error(Error)
    end.

revoke(?CLI_PREFIX(["revoke" | Revokes]), [], []) ->
    revoke(Revokes).

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
               parse_roles(Users)).

ciphers(?CLI_PREFIX(["ciphers"]), [], []) ->
    case riak_core_security:format_ciphers() of
        {Cfgd, Valid} ->
            [ clique_status:text(C) || C <- [Cfgd, Valid]];
        {Cfgd, Valid, Invalid} ->
            [ clique_status:text(C) || C <- [Cfgd, Valid, Invalid]]
    end;

ciphers(?CLI_PREFIX(["ciphers", CipherList]), [], []) ->
    case riak_core_security:set_ciphers(CipherList) of
        ok ->
            %% TODO This will get neater after refactoring all the patterns in
            %% the module
            ciphers(?CLI_PREFIX(["ciphers"]), [], []);
        {error, _} = Error ->
            fmt_error(Error)
    end.

% TODO This needs to die. I think.
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
