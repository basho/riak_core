-define(PT_MSG_INIT, 0).
-define(PT_MSG_OBJ, 1).
-define(PT_MSG_OLDSYNC, 2).
-define(PT_MSG_SYNC, 3).
-define(PT_MSG_CONFIGURE, 4).

%% external information for handoffs
-record(handoff,
        { module :: atom(),
          index  :: integer(),
          node   :: atom(),
          type   :: primary | fallback
        }).

