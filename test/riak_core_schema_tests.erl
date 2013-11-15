-module(riak_core_schema_tests).

-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

%% basic schema test will check to make sure that all defaults from the schema
%% make it into the generated app.config
basic_schema_test() ->
    %% The defaults are defined in ../priv/riak_core.schema. it is the file under test. 
    Config = cuttlefish_unit:generate_templated_config("../priv/riak_core.schema", [], context()),

    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.n_val", 3),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.pr", 0),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.r", quorum),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.w", quorum),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.pw", 0),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.dw", quorum),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.rw", quorum),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.allow_mult", true),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.last_write_wins", false),


    cuttlefish_unit:assert_config(Config, "riak_core.ring_creation_size", 64),
    cuttlefish_unit:assert_config(Config, "riak_core.ring_state_dir", "./ring"),
    cuttlefish_unit:assert_config(Config, "riak_core.handoff_port", 8099 ),
    cuttlefish_unit:assert_config(Config, "riak_core.dtrace_support", false),
    cuttlefish_unit:assert_config(Config, "riak_core.platform_bin_dir",  "./bin"),
    cuttlefish_unit:assert_config(Config, "riak_core.platform_data_dir", "./data"),
    cuttlefish_unit:assert_config(Config, "riak_core.platform_etc_dir",  "./etc"),
    cuttlefish_unit:assert_config(Config, "riak_core.platform_lib_dir",  "./lib"),
    cuttlefish_unit:assert_config(Config, "riak_core.platform_log_dir",  "./log"),
    ok.

default_bucket_properties_test() ->
    Conf = [
        {["buckets", "default", "pr"], "quorum"},
        {["buckets", "default", "rw"], "all"},
        {["buckets", "default", "w"], "1"},
        {["buckets", "default", "r"], "3"},
        {["buckets", "default", "siblings"], off},
        {["buckets", "default", "last_write_wins"], true}
    ],

    Config = cuttlefish_unit:generate_templated_config(
        "../priv/riak_core.schema", Conf, context()),

    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.pr", quorum),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.rw", all),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.w", 1),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.r", 3),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.allow_mult", false),
    cuttlefish_unit:assert_config(Config, "riak_core.default_bucket_props.last_write_wins", true),
    ok.

override_schema_test() ->
    %% Conf represents the riak.conf file that would be read in by cuttlefish.
    %% this proplists is what would be output by the conf_parse module
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

%% this context() represents the substitution variables that rebar will use during the build process.
%% riak_core's schema file is written with some {{mustache_vars}} for substitution during packaging
%% cuttlefish doesn't have a great time parsing those, so we perform the substitutions first, because
%% that's how it would work in real life.
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
