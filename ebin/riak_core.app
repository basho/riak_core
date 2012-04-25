%% -*- tab-width: 4;erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% ex: ts=4 sw=4 et
{application, riak_core,
[
  {description, "Riak Core"},
  {vsn, "1.1.2"},
  {modules, [
             app_helper,
             bloom,
             chash,
             gen_nb_server,
             gen_server2,
             json_pp,
             merkerl,
             priority_queue,
             process_proxy,
             riak_core_gossip_legacy,
             riak_core,
             riak_core_apl,
             riak_core_app,
             riak_core_bucket,
             riak_core_cinfo_core,
             riak_core_claim,
             riak_core_new_claim,
             riak_core_config,
             riak_core_console,
             riak_core_coverage_fsm,
             riak_core_coverage_plan,
             riak_core_eventhandler_guard,
             riak_core_eventhandler_sup,
             riak_core_format,
             riak_core_gossip,
             riak_core_handoff_listener,
             riak_core_handoff_listener_sup,
             riak_core_handoff_manager,
             riak_core_handoff_receiver,
             riak_core_handoff_receiver_sup,
             riak_core_handoff_sender,
             riak_core_handoff_sender_sup,
             riak_core_handoff_sup,
             riak_core_nodeid,
             riak_core_node_watcher,
             riak_core_node_watcher_events,
             riak_core_pb,
             riak_core_ring,
             riak_core_ring_events,
             riak_core_ring_handler,
             riak_core_ring_manager,
             riak_core_ring_util,
             riak_core_stat,
             riak_core_status,
             riak_core_sup,
             riak_core_sysmon_handler,
             riak_core_sysmon_minder,
             riak_core_tracer,
             riak_core_test_util,
             riak_core_util,
             riak_core_vnode,
             riak_core_vnode_manager,
             riak_core_vnode_master,
             riak_core_vnode_proxy,
             riak_core_vnode_proxy_sup,
             riak_core_vnode_sup,
             riak_core_vnode_worker,
             riak_core_vnode_worker_pool,
             riak_core_web,
             riak_core_wm_urlmap,
             slide,
             spiraltime,
             supervisor_pre_r14b04,
             vclock
            ]},
  {registered, []},
  {applications, [
                  kernel,
                  stdlib,
                  lager,
                  sasl,
                  crypto,
                  riak_sysmon,
                  webmachine,
                  os_mon
                 ]},
  {mod, { riak_core_app, []}},
  {env, [
         %% Cluster name
         {cluster_name, "default"},

         %% Default location of ringstate
         {ring_state_dir, "data/ring"},

         %% Default ring creation size.  Make sure it is a power of 2,
         %% e.g. 16, 32, 64, 128, 256, 512 etc
         {ring_creation_size, 64},

         %% Default gossip interval (milliseconds)
         {gossip_interval, 60000},

         %% Target N value
         {target_n_val, 4},

         %% Default claims functions
         {wants_claim_fun, {riak_core_claim, default_wants_claim}},
         {choose_claim_fun, {riak_core_claim, default_choose_claim}},

         %% Vnode inactivity timeout (how often to check if fallback vnodes
         %% should return their data) in ms.
         {vnode_inactivity_timeout, 60000},

         %% Number of VNodes allowed to do handoff concurrently.
         {handoff_concurrency, 1},

         %% Disable Nagle on HTTP sockets
         {disable_http_nagle, false},

         %% Handoff IP/port
         {handoff_port, 8099},
         {handoff_ip, "0.0.0.0"}
        ]}
 ]}.
