-module(riak_core_security_tests).
-compile(export_all).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

start_manager(Node) ->
    Dir = atom_to_list(Node),
    case length(Dir) of
        0 -> exit(no_dir); %% make sure we don't do something stupid below
        _ -> ok
    end,
    os:cmd("mkdir " ++ Dir),
    os:cmd("rm -rf " ++ Dir ++ "/"),
    {ok, Mgr} = riak_core_metadata_manager:start_link([{data_dir, Dir},
                                                       {node_name, Node}]),
    TreeDir = Dir ++ "/trees",
    {ok, Tree} = riak_core_metadata_hashtree:start_link(TreeDir),
    {ok, Bcst} = riak_core_broadcast:start_link([node()], [], [], []),
    unlink(Bcst),
    unlink(Tree),
    unlink(Mgr),

    application:set_env(riak_core, permissions, [{riak_kv,[get,put]}]),
    {Mgr, Tree, Bcst}.

stop_manager({Mgr, Tree, Bcst}) ->
    catch exit(Mgr, kill),
    catch exit(Tree, kill),
    catch exit(Bcst, kill),
    ok.

security_test_() ->
    {foreach,
     fun() ->
             start_manager(node())
     end,
     fun(S) ->
             stop_manager(S)
     end,
     [{timeout, 60, { "find_user", fun test_find_user/0 }},
      {timeout, 60, { "test_find_bucket_grants", fun test_find_bucket_grants/0 }},
      {timeout, 60, { "find_one_user_by_metadata", fun test_find_one_user_by_metadata/0 }},
      {timeout, 60, { "find_unique_user_by_metadata", fun test_find_unique_user_by_metadata/0 }},
      {timeout, 60, { "trust auth works",
                      fun() ->
                              ?assertMatch({error, _}, riak_core_security:authenticate(<<"user">>, <<"password">>,
                                                                                       [{ip, {127, 0, 0, 1}}])),
                              ?assertEqual(ok, riak_core_security:add_user(<<"user">>,
                                                                           [{"password","password"}])),
                              ?assertMatch({error, _}, riak_core_security:authenticate(<<"user">>, <<"password">>,
                                                                                       [{ip, {127, 0, 0, 1}}])),
                              ?assertEqual(ok, riak_core_security:add_source(all, {{127, 0, 0, 1}, 32}, trust, [])),
                              ?assertMatch({ok, _}, riak_core_security:authenticate(<<"user">>, <<"password">>,
                                                                                    [{ip, {127, 0, 0, 1}}])),
                              %% make sure these don't crash, at least
                              ?assertEqual(ok, riak_core_security:print_users()),
                              ?assertEqual(ok, riak_core_security:print_sources()),
                              ?assertEqual(ok, riak_core_security:print_user(<<"user">>)),
                              ok
                      end}},
     {timeout, 60, { "password auth works",
                     fun() ->
                             ?assertMatch({error, _}, riak_core_security:authenticate(<<"user">>, <<"password">>,
                                                                                      [{ip, {127, 0, 0, 1}}])),
                             ?assertEqual(ok, riak_core_security:add_user(<<"user">>,
                                                                          [])),
                             ?assertMatch({error, _}, riak_core_security:authenticate(<<"user">>, <<"password">>,
                                                                                      [{ip, {127, 0, 0, 1}}])),
                             ?assertEqual(ok, riak_core_security:add_source(all, {{127, 0, 0, 1}, 32}, password, [])),
                             ?assertEqual({error, missing_password}, riak_core_security:authenticate(<<"user">>, <<"password">>,
                                                                                                     [{ip, {127, 0, 0, 1}}])),
                             ?assertEqual(ok, riak_core_security:alter_user(<<"user">>, [{"password", "password"}])),
                             ?assertMatch({error, _}, riak_core_security:authenticate(<<"user">>, <<"badpassword">>,
                                                                                      [{ip, {127, 0, 0, 1}}])),
                             ?assertMatch({ok, _}, riak_core_security:authenticate(<<"user">>, <<"password">>,
                                                                                   [{ip, {127, 0, 0, 1}}])),
                             %% make sure these don't crash, at least
                             ?assertEqual(ok, riak_core_security:print_users()),
                             ?assertEqual(ok, riak_core_security:print_sources()),
                             ?assertEqual(ok, riak_core_security:print_user(<<"user">>)),
                             ok
                     end}},
     {timeout, 60, { "user grant/revoke on type/bucket works",
                     fun() ->
                             ?assertEqual(ok, riak_core_security:add_user(<<"user">>,
                                                                          [{"password","password"}])),
                             ?assertEqual(ok, riak_core_security:add_source(all, {{127, 0, 0, 1}, 32}, password, [])),
                             {ok, Ctx} = riak_core_security:authenticate(<<"user">>, <<"password">>,
                                                                         [{ip, {127, 0, 0, 1}}]),
                             ?assertMatch({false, _,  _}, riak_core_security:check_permissions({"riak_kv.get", {<<"default">>, <<"mybucket">>}}, Ctx)),
                             ?assertEqual(ok, riak_core_security:add_grant([<<"user">>], {<<"default">>, <<"mybucket">>}, ["riak_kv.get", "riak_kv.put"])),
                             ?assertMatch({error, {unknown_permission, _}}, riak_core_security:add_grant([<<"user">>], {<<"default">>, <<"mybucket">>}, ["riak_kv.upsert"])),
                             ?assertMatch({true, _}, riak_core_security:check_permissions({"riak_kv.get", {<<"default">>, <<"mybucket">>}}, Ctx)),
                             ?assertEqual(ok, riak_core_security:add_revoke([<<"user">>], {<<"default">>, <<"mybucket">>}, ["riak_kv.get"])),
                             ?assertMatch({error, {unknown_permission, _}}, riak_core_security:add_revoke([<<"user">>], {<<"default">>, <<"mybucket">>}, ["riak_kv.upsert"])),
                             ?assertMatch({false, _, _}, riak_core_security:check_permissions({"riak_kv.get", {<<"default">>, <<"mybucket">>}}, Ctx)),
                             ?assertMatch({true, _}, riak_core_security:check_permissions({"riak_kv.put", {<<"default">>, <<"mybucket">>}}, Ctx)),
                             %% make sure these don't crash, at least
                             ?assertEqual(ok, riak_core_security:print_users()),
                             ?assertEqual(ok, riak_core_security:print_sources()),
                             ?assertEqual(ok, riak_core_security:print_user(<<"user">>)),
                             %% delete the user
                             ?assertMatch(ok, riak_core_security:del_user(<<"user">>)),
                             %% re-add them
                             ?assertEqual(ok, riak_core_security:add_user(<<"user">>,
                                                                          [{"password","password"}])),
                             %% make sure their old grants are gone
                             {ok, Ctx2} = riak_core_security:authenticate(<<"user">>, <<"password">>,
                                                                          [{ip, {127, 0, 0, 1}}]),
                             ?assertMatch({false, _,  _}, riak_core_security:check_permissions({"riak_kv.put", {<<"default">>, <<"mybucket">>}}, Ctx2)),
                             ok
                     end}},
     {timeout, 60, { "group grant/revoke on type/bucket works",
                     fun() ->
                             ?assertEqual(ok, riak_core_security:add_user(<<"user">>,
                                                                          [{"password","password"}])),
                             ?assertEqual(ok, riak_core_security:add_group(<<"group">>,
                                                                           [])),
                             ?assertEqual(ok, riak_core_security:add_source([<<"user">>], {{127, 0, 0, 1}, 32}, password, [])),
                             {ok, Ctx} = riak_core_security:authenticate(<<"user">>, <<"password">>,
                                                                         [{ip, {127, 0, 0, 1}}]),
                             ?assertMatch({false, _,  _}, riak_core_security:check_permissions({"riak_kv.get", {<<"default">>, <<"mybucket">>}}, Ctx)),
                             ?assertEqual(ok, riak_core_security:add_grant([<<"group">>], {<<"default">>, <<"mybucket">>}, ["riak_kv.get"])),
                             ?assertMatch({error, {unknown_permission, _}}, riak_core_security:add_grant([<<"group">>], {<<"default">>, <<"mybucket">>}, ["riak_kv.upsert"])),
                             ?assertMatch({false, _, _}, riak_core_security:check_permissions({"riak_kv.get", {<<"default">>, <<"mybucket">>}}, Ctx)),
                             ?assertEqual(ok, riak_core_security:alter_user(<<"user">>, [{"groups", ["group"]}])),
                             ?assertMatch({true, _}, riak_core_security:check_permissions({"riak_kv.get", {<<"default">>, <<"mybucket">>}}, Ctx)),
                             ?assertEqual(ok, riak_core_security:add_revoke([<<"group">>], {<<"default">>, <<"mybucket">>}, ["riak_kv.get"])),
                             ?assertMatch({error, {unknown_permission, _}}, riak_core_security:add_revoke([<<"group">>], {<<"default">>, <<"mybucket">>}, ["riak_kv.upsert"])),
                             ?assertMatch({false, _, _}, riak_core_security:check_permissions({"riak_kv.get", {<<"default">>, <<"mybucket">>}}, Ctx)),
                             ?assertEqual(ok, riak_core_security:add_grant([<<"group">>], {<<"default">>, <<"mybucket">>}, ["riak_kv.get"])),
                             ?assertMatch({true, _}, riak_core_security:check_permissions({"riak_kv.get", {<<"default">>, <<"mybucket">>}}, Ctx)),
                             ?assertMatch({error, {unknown_groups, _}}, riak_core_security:alter_user(<<"user">>, [{"groups", ["nogroup"]}])),
                             ?assertEqual(ok, riak_core_security:alter_user(<<"user">>, [{"groups", []}])),
                             ?assertMatch({false, _, _}, riak_core_security:check_permissions({"riak_kv.get", {<<"default">>, <<"mybucket">>}}, Ctx)),
                             ?assertEqual(ok, riak_core_security:alter_user(<<"user">>, [{"groups", ["group"]}])),
                             %% make sure these don't crash, at least
                             ?assertEqual(ok, riak_core_security:print_users()),
                             ?assertEqual(ok, riak_core_security:print_sources()),
                             ?assertEqual(ok, riak_core_security:print_user(<<"user">>)),
                             ?assertEqual(ok, riak_core_security:print_groups()),
                             ?assertEqual(ok, riak_core_security:print_group(<<"group">>)),
                             ok
                     end}},
     {timeout, 60, { "all grant/revoke on type/bucket works",
                     fun() ->
                             ?assertEqual(ok, riak_core_security:add_user(<<"user">>,
                                                                          [{"password","password"}])),
                             ?assertEqual(ok, riak_core_security:add_source([<<"user">>], {{127, 0, 0, 1}, 32}, password, [])),
                             {ok, Ctx} = riak_core_security:authenticate(<<"user">>, <<"password">>,
                                                                         [{ip, {127, 0, 0, 1}}]),
                             ?assertMatch({false, _,  _}, riak_core_security:check_permissions({"riak_kv.get", {<<"default">>, <<"mybucket">>}}, Ctx)),
                             ?assertEqual(ok, riak_core_security:add_grant(all, {<<"default">>, <<"mybucket">>}, ["riak_kv.get"])),
                             ?assertMatch({error, {unknown_permission, _}}, riak_core_security:add_grant(all, {<<"default">>, <<"mybucket">>}, ["riak_kv.upsert"])),
                             ?assertMatch({true, _}, riak_core_security:check_permissions({"riak_kv.get", {<<"default">>, <<"mybucket">>}}, Ctx)),
                             ?assertEqual(ok, riak_core_security:add_revoke(all, {<<"default">>, <<"mybucket">>}, ["riak_kv.get"])),
                             ?assertMatch({false, _,  _}, riak_core_security:check_permissions({"riak_kv.get", {<<"default">>, <<"mybucket">>}}, Ctx)),
                             %% make sure these don't crash, at least
                             ?assertEqual(ok, riak_core_security:print_users()),
                             ?assertEqual(ok, riak_core_security:print_sources()),
                             ?assertEqual(ok, riak_core_security:print_user(<<"user">>)),
                             ?assertEqual(ok, riak_core_security:print_groups()),
                             ?assertEqual({error, {unknown_group, <<"all">>}}, riak_core_security:print_group(<<"all">>)),
                             ok
                     end}},
     {timeout, 60, { "groups can be members of groups and inherit permissions from them",
                     fun() ->
                             ?assertEqual(ok, riak_core_security:add_group(<<"sysadmin">>, [])),
                             ?assertEqual(ok, riak_core_security:add_group(<<"superuser">>, [{"groups", ["sysadmin"]}])),
                             ?assertEqual(ok, riak_core_security:add_user(<<"user">>,
                                                                          [{"password","password"}])),
                             ?assertEqual(ok, riak_core_security:add_source([<<"user">>], {{127, 0, 0, 1}, 32}, password, [])),
                             %% sysadmins can get/put on any key in a default bucket
                             ?assertEqual(ok, riak_core_security:add_grant([<<"sysadmin">>], <<"default">>, ["riak_kv.get", "riak_kv.put"])),
                             %% authenticating from the wrong IP
                             ?assertMatch({error, no_matching_sources}, riak_core_security:authenticate(<<"user">>, <<"password">>,
                                                                                                        [{ip, {10, 0, 0, 1}}])),
                             {ok, Ctx} = riak_core_security:authenticate(<<"user">>, <<"password">>,
                                                                         [{ip, {127, 0, 0, 1}}]),
                             ?assertMatch({false, _, _}, riak_core_security:check_permissions({"riak_kv.get", {<<"default">>, <<"mybucket">>}}, Ctx)),
                             ?assertMatch({false, _, _}, riak_core_security:check_permissions({"riak_kv.put", {<<"default">>, <<"myotherbucket">>}}, Ctx)),
                             ?assertEqual(ok, riak_core_security:alter_user(<<"user">>, [{"groups", ["superuser"]}])),
                             ?assertMatch({true, _}, riak_core_security:check_permissions({"riak_kv.get", {<<"default">>, <<"mybucket">>}}, Ctx)),
                             ?assertMatch({true, _}, riak_core_security:check_permissions({"riak_kv.put", {<<"default">>, <<"myotherbucket">>}}, Ctx)),
                             %% make sure these don't crash, at least
                             ?assertEqual(ok, riak_core_security:print_users()),
                             ?assertEqual(ok, riak_core_security:print_sources()),
                             ?assertEqual(ok, riak_core_security:print_user(<<"user">>)),
                             ?assertEqual(ok, riak_core_security:print_grants(<<"user">>)),
                             ?assertEqual(ok, riak_core_security:print_groups()),
                             ?assertEqual(ok, riak_core_security:print_group(<<"superuser">>)),
                             ?assertEqual(ok, riak_core_security:print_group(<<"sysadmin">>)),
                             ?assertEqual(ok, riak_core_security:alter_group(<<"superuser">>, [{"groups", []}])),
                             ?assertMatch({false, _, _}, riak_core_security:check_permissions({"riak_kv.get", {<<"default">>, <<"mybucket">>}}, Ctx)),
                             ?assertEqual(ok, riak_core_security:alter_group(<<"superuser">>, [{"groups", ["sysadmin"]}])),
                             ?assertMatch({true, _}, riak_core_security:check_permissions({"riak_kv.get", {<<"default">>, <<"mybucket">>}}, Ctx)),
                             ?assertMatch(ok, riak_core_security:del_group(<<"sysadmin">>)),
                             ?assertMatch({false, _, _}, riak_core_security:check_permissions({"riak_kv.get", {<<"default">>, <<"mybucket">>}}, Ctx)),
                             %% check re-adding the group does not resurrect old permissions or memberships
                             ?assertEqual(ok, riak_core_security:add_group(<<"sysadmin">>, [])),
                             ?assertMatch({false, _, _}, riak_core_security:check_permissions({"riak_kv.get", {<<"default">>, <<"mybucket">>}}, Ctx)),
                             ?assertEqual(ok, riak_core_security:add_grant([<<"sysadmin">>], <<"default">>, ["riak_kv.get", "riak_kv.put"])),
                             ?assertMatch({false, _, _}, riak_core_security:check_permissions({"riak_kv.get", {<<"default">>, <<"mybucket">>}}, Ctx)),
                             %% re-adding the group membership does restore the permissions, though
                             ?assertEqual(ok, riak_core_security:alter_group(<<"superuser">>, [{"groups", ["sysadmin"]}])),
                             ?assertMatch({true, _}, riak_core_security:check_permissions({"riak_kv.get", {<<"default">>, <<"mybucket">>}}, Ctx)),
                             ok
                     end}},
     {timeout, 60, { "user/group disambiguation",
                     fun() ->
                             ?assertEqual(ok, riak_core_security:add_group(<<"sysadmin">>, [])),
                             ?assertEqual(ok, riak_core_security:add_user(<<"sysadmin">>, [{"password", "password"}])),
                             ?assertEqual(ok, riak_core_security:add_source(all, {{127, 0, 0, 1}, 32}, password, [])),
                             ?assertEqual({error, {duplicate_roles, [<<"sysadmin">>]}}, riak_core_security:add_grant([<<"sysadmin">>], <<"default">>, ["riak_kv.get", "riak_kv.put"])),
                             ?assertEqual(ok, riak_core_security:add_grant([<<"user/sysadmin">>], <<"default">>, ["riak_kv.get", "riak_kv.put"])),
                             ?assertEqual(ok, riak_core_security:add_grant([<<"group/sysadmin">>], any, ["riak_kv.get", "riak_kv.put"])),
                             {ok, Ctx} = riak_core_security:authenticate(<<"sysadmin">>, <<"password">>,
                                                                         [{ip, {127, 0, 0, 1}}]),
                             ?assertMatch({true, _}, riak_core_security:check_permissions({"riak_kv.get", {<<"default">>, <<"mybucket">>}}, Ctx)),
                             ?assertMatch({false, _, _}, riak_core_security:check_permissions({"riak_kv.get", {<<"custom">>, <<"mybucket">>}}, Ctx)),
                             ?assertEqual(ok, riak_core_security:alter_user(<<"sysadmin">>, [{"groups", ["sysadmin"]}])),
                             ?assertMatch({true, _}, riak_core_security:check_permissions({"riak_kv.get", {<<"custom">>, <<"mybucket">>}}, Ctx)),
                             ?assertMatch({true, _}, riak_core_security:check_permissions({"riak_kv.get"}, Ctx)),
                             ?assertEqual(ok, riak_core_security:print_grants(<<"sysadmin">>)),
                             ok
                     end}},
     {timeout, 60, { "Expected user/group errors",
                     fun() ->
                             ?assertMatch({error, _}, riak_core_security:add_group(<<"all">>, [])),
                             ?assertMatch({error, _}, riak_core_security:alter_user(<<"sysadmin">>, [{"password", "password"}])),
                             ?assertMatch({error, _}, riak_core_security:print_user(<<"sysadmin">>)),
                             ?assertMatch({error, _}, riak_core_security:alter_group(<<"sysadmin">>, [{"password", "password"}])),
                             ?assertMatch({error, _}, riak_core_security:print_group(<<"sysadmin">>)),
                             ?assertEqual(ok, riak_core_security:add_group(<<"sysadmin">>, [])),
                             ?assertMatch({error, _}, riak_core_security:add_group(<<"sysadmin">>, [])),
                             ?assertEqual(ok, riak_core_security:add_user(<<"sysadmin">>, [])),
                             ?assertEqual({error, {duplicate_roles, [<<"sysadmin">>]}}, riak_core_security:add_grant([<<"sysadmin">>], <<"default">>, ["riak_kv.get", "riak_kv.put"])),
                             ?assertMatch({error, {unknown_roles, [<<"sysadm">>]}}, riak_core_security:add_grant([<<"sysadm">>], any, ["riak_kv.put"])),
                             ?assertMatch({error, {unknown_permission, _}}, riak_core_security:add_grant([<<"group/sysadmin">>], any, ["riak_kv.upsert"])),
                             ?assertMatch({errors, [_, _]}, riak_core_security:add_grant([<<"group/foo">>], any, ["riak_kv.upsert"])),
                             ?assertMatch({errors, [_, _]}, riak_core_security:add_grant([<<"sysadmin">>], any, ["riak_kv.upsert"])),
                             ?assertMatch({error, {unknown_role, _}}, riak_core_security:print_grants(<<"foo">>)),
                             ?assertEqual(ok, riak_core_security:add_user(<<"fred">>, [])),
                             ?assertMatch({error, {unknown_role, _}}, riak_core_security:print_grants(<<"group/fred">>)),
                             ok
                     end}}
    ]}.

test_find_bucket_grants() ->
    ok = riak_core_security:add_group("testgroup", []),
    ok = riak_core_security:add_user("testuser1", [{groups, "testgroup"}]),
    ok = riak_core_security:add_user("testuser2", []),
    ok = riak_core_security:add_grant(["testuser1", "testuser2"], <<"bucket">>, ["riak_kv.get"]),
    ok = riak_core_security:add_grant(["testuser2"], <<"bucket">>, ["riak_kv.put"]),
    ok = riak_core_security:add_grant(all, <<"bucket">>, ["riak_kv.get"]),
    ok = riak_core_security:add_grant(["group/testgroup"], <<"bucket">>, ["riak_kv.put"]),
    Grants = riak_core_security:find_bucket_grants(<<"bucket">>, user),
    GroupGrants = riak_core_security:find_bucket_grants(<<"bucket">>, group),
    ?assertMatch({_, ["riak_kv.get"]}, lists:keyfind("testuser1", 1, Grants)),
    {_, Perms} = lists:keyfind("testuser2", 1, Grants),
    ?assertEqual(lists:sort(["riak_kv.get", "riak_kv.put"]), lists:sort(Perms)),
    ?assertMatch({_, ["riak_kv.get"]}, lists:keyfind(all, 1, GroupGrants)),
    ?assertMatch({_, ["riak_kv.put"]}, lists:keyfind("testgroup", 1, GroupGrants)).

test_find_user() ->
    Options = [{key, value}],
    Username = "testuser",
    ok = riak_core_security:add_user(Username, Options),
    ?assertMatch(Options, riak_core_security:find_user(Username)).

test_find_one_user_by_metadata() ->
    ok = riak_core_security:add_user("paul", [{"key_and_value", "match"}]),
    ?assertMatch({<<"paul">>, _Options},
                 riak_core_security:find_one_user_by_metadata("key_and_value", "match")),
    ?assertMatch({error, not_found},
             riak_core_security:find_one_user_by_metadata("no", "match")).

-endif.

test_find_unique_user_by_metadata() ->
    ?assertMatch({error, not_found},
                 riak_core_security:find_unique_user_by_metadata("key", "val")),
    ok = riak_core_security:add_user("user1", [{"key", "val"}]),
    ?assertMatch({<<"user1">>, _Options},
                 riak_core_security:find_unique_user_by_metadata("key", "val")),
    ok = riak_core_security:add_user("user2", [{"key", "val"}]),
    ?assertMatch({error, not_unique},
                 riak_core_security:find_unique_user_by_metadata("key", "val")).
