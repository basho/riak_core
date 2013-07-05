-module(riak_core_security).

%% TODO
-compile(export_all).

initial_config() ->
    %[{users, [{"andrew", [{password, "foo"}]}]},
    [{users, []},
     {sources, []},
     {grants, []},
     {revokes, []}
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

%% TODO a context should only be valid for a short time, eg. 5 seconds.
%% Or perhaps every grant change should increment some value on the user
%% information so we can detect when we need to re-pull the context.
get_context(Username, Meta) ->
    Empty = term_to_binary([]),
    Grants = lookup(Username, lookup(grants, Meta, []), Empty),
    Revokes = lookup(Username, lookup(revokes, Meta, []), Empty),
    %% XXX this is more or less pseudocode since we don't have a structure for
    %% grants yet
    lists:keymerge(1, binary_to_term(Grants), binary_to_term(Revokes)).

authenticate(Username, Password, PeerIP) ->
    Meta = get_meta(),
    Users = lookup(users, Meta, []),
    case lookup(Username, Users) of
        undefined ->
            {error, unknown_user};
        UserData ->
            Sources = sort_sources(lookup(sources, Meta, [])),
            case match_source(Sources, Username, PeerIP) of
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
    {ok, Options}.

