
-record(handoff, { module        :: atom(),
                   index         :: integer(),
                   target_node   :: atom(),
                   vnode_pid     :: pid(),
                   sender_pid    :: pid(),
                   restarts      :: integer(),
                   timestamp     :: tuple(),
                   status        :: any()
                 }).

-define(PT_MSG_INIT, 0).
-define(PT_MSG_OBJ, 1).
-define(PT_MSG_OLDSYNC, 2).
-define(PT_MSG_SYNC, 3).
-define(PT_MSG_CONFIGURE, 4).
