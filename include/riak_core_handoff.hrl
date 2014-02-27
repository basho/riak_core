-define(PT_MSG_INIT, 0).
-define(PT_MSG_OBJ, 1).
-define(PT_MSG_OLDSYNC, 2).
-define(PT_MSG_SYNC, 3).
-define(PT_MSG_CONFIGURE, 4).
-define(PT_MSG_BATCH, 5).

-record(ho_stats,
        {
          interval_end          :: erlang:timestamp(),
          last_update           :: erlang:timestamp(),
          objs=0                :: non_neg_integer(),
          bytes=0               :: non_neg_integer()
        }).

-type ho_stats() :: #ho_stats{}.
-type ho_type() :: ownership_handoff | hinted_handoff | repair | resize_transfer.
-type predicate() :: fun((any()) -> boolean()).

-type index() :: chash:index_as_int().
-type mod_src_tgt() :: {module(), index(), index()} | {undefined, undefined, undefined}.
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
          type                  :: ho_type(),
          req_origin            :: node(),
          filter_mod_fun        :: {module(), atom()},
          size                  :: {function(), dynamic} | {non_neg_integer(), bytes | objects}
        }).
-type handoff_status() :: #handoff_status{}.
