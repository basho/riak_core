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
