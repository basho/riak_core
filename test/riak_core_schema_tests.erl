-module(riak_core_schema_tests).

-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

basic_schema_test() ->
    Config = cuttlefish_unit:generate_templated_config("../priv/riak_core.schema", [], context()),

    cuttlefish_unit:assert_config(Config, "riak_core.ring_creation_size", 64),
    cuttlefish_unit:assert_config(Config, "riak_core.ring_state_dir", "./ring"),
    cuttlefish_unit:assert_config(Config, "riak_core.handoff_port", 8099 ),
    cuttlefish_unit:assert_config(Config,"riak_core.dtrace_support", false),
    cuttlefish_unit:assert_config(Config, "riak_core.platform_bin_dir",  "./bin"),
    cuttlefish_unit:assert_config(Config, "riak_core.platform_data_dir", "./data"),
    cuttlefish_unit:assert_config(Config, "riak_core.platform_etc_dir",  "./etc"),
    cuttlefish_unit:assert_config(Config, "riak_core.platform_lib_dir",  "./lib"),
    cuttlefish_unit:assert_config(Config, "riak_core.platform_log_dir",  "./log"),
    ok.

override_schema_test() ->
    Conf = [
        {["ring_size"], 8},
        {["ring", "state_dir"], "/absolute/ring"},
        {["handoff", "port"], 8888},
        {["dtrace"], on},
        %% Platform-specific installation paths (substituted by rebar)
        {["platform_bin_dir"], "/absolute/bin"},
        {["platform_data_dir"],"/absolute/data" },
        {["platform_etc_dir"], "/absolute/etc"},
        {["platform_lib_dir"], "/absolute/lib"},
        {["platform_log_dir"], "/absolute/log"},

        %% Optional
        {["ssl", "certfile"], "/absolute/etc/cert.pem"},
        {["ssl", "keyfile"], "/absolute/etc/key.pem"},
        {["ssl", "cacertfile"], "/absolute/etc/cacertfile.pem"},
        {["handoff", "ssl", "certfile"], "/tmp/erlserver.pem"},
        {["handoff", "ssl", "keyfile"], "/tmp/erlkey/pem"}
    ],

    Config = cuttlefish_unit:generate_templated_config("../priv/riak_core.schema", Conf, context()),

    cuttlefish_unit:assert_config(Config, "riak_core.ring_creation_size", 8),
    cuttlefish_unit:assert_config(Config, "riak_core.ring_state_dir", "/absolute/ring"),
    cuttlefish_unit:assert_config(Config, "riak_core.handoff_port", 8888),
    cuttlefish_unit:assert_config(Config, "riak_core.dtrace_support", true),
    cuttlefish_unit:assert_config(Config, "riak_core.platform_bin_dir", "/absolute/bin"),
    cuttlefish_unit:assert_config(Config, "riak_core.platform_data_dir", "/absolute/data"),
    cuttlefish_unit:assert_config(Config, "riak_core.platform_etc_dir", "/absolute/etc"),
    cuttlefish_unit:assert_config(Config, "riak_core.platform_lib_dir", "/absolute/lib"),
    cuttlefish_unit:assert_config(Config, "riak_core.platform_log_dir", "/absolute/log"),
    cuttlefish_unit:assert_config(Config, "riak_core.ssl.certfile", "/absolute/etc/cert.pem"),
    cuttlefish_unit:assert_config(Config, "riak_core.ssl.keyfile", "/absolute/etc/key.pem"),
    cuttlefish_unit:assert_config(Config, "riak_core.ssl.cacertfile", "/absolute/etc/cacertfile.pem"),
    cuttlefish_unit:assert_config(Config, "riak_core.handoff_ssl_options.certfile", "/tmp/erlserver.pem"),
    cuttlefish_unit:assert_config(Config, "riak_core.handoff_ssl_options.keyfile", "/tmp/erlkey/pem"),
    ok.

context() ->
    [
        {handoff_port, "8099"},
        {ring_state_dir , "./ring"},
        {platform_bin_dir , "./bin"},
        {platform_data_dir, "./data"},
        {platform_etc_dir , "./etc"},
        {platform_lib_dir , "./lib"},
        {platform_log_dir , "./log"}
    ].
