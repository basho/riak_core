-define(PT_MSG_INIT, 0).
-define(PT_MSG_OBJ, 1).
-define(PT_MSG_OLDSYNC, 2).
-define(PT_MSG_SYNC, 3).
-define(PT_MSG_CONFIGURE, 4).

-record(ho_stats,
        {
          interval_end          :: erlang:timestamp(),
          last_update           :: erlang:timestamp(),
          objs=0                :: non_neg_integer(),
          bytes=0               :: non_neg_integer()
        }).

-type ho_stats() :: #ho_stats{}.
-type ho_type() :: ownership_handoff | hinted_handoff.
-type predicate() :: fun((any()) -> boolean()).

-type index() :: integer().
-type mod_src_tgt() :: {module(), index(), index()}.
-type mod_partition() :: {module(), index()}.

-record(handoff_status,
        { mod_src_tgt           :: mod_src_tgt(),
          src_node              :: node(),
          target_node           :: node(),
          direction             :: inbound | outbound,
          transport_pid         :: pid(),
          transport_mon         :: reference(),
          timestamp             :: tuple(),
          status                :: any(),
          stats                 :: dict(),
          vnode_pid             :: pid() | undefined,
          vnode_mon             :: reference(),
          type                  :: ownership | hinted_handoff | repair,
          req_origin            :: node(),
          filter_mod_fun        :: {module(), atom()}
        }).
-type handoff() :: #handoff_status{}.
-type handoffs() :: [handoff()].
