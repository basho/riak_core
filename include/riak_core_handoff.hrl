-define(PT_MSG_INIT, 0).
-define(PT_MSG_OBJ, 1).
-define(PT_MSG_OLDSYNC, 2).
-define(PT_MSG_SYNC, 3).
-define(PT_MSG_CONFIGURE, 4).

%% information about a sender process
-record(sender,
        { module      :: atom(),
          index       :: integer(),
          target_node :: atom(),
          vnode_pid   :: pid()
        }).

%% information about a receiver process
-record(receiver,
        { ssl_opts   :: [any()]
        }).

%% all information being tracked about a given handoff (active or pending)
-record(handoff,
        { sender     :: #sender{} | undefined,
          receiver   :: #receiver{} | undefined,
          pid        :: pid(),
          timestamp  :: tuple(),
          status     :: any()
        }).

