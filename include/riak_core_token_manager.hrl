-type tm_token() :: any().
-type tm_meta()  :: {atom(), any()}.                %% meta data to associate with a token
-type tm_period() :: pos_integer().                 %% refill period in milliseconds
-type tm_count() :: pos_integer().                  %% refill tokens to count at each refill period
-type tm_rate() :: {tm_period(), tm_count()}.       %% token refresh rate
-type tm_stat_event() :: refill_event | give_event. %% stat event type

%% Results of a "ps" of live given or blocked tokens
-record(tm_stat_live,
        {
          token      :: tm_token(),               %% token type
          consumer   :: pid(),                    %% process asking for token
          meta       :: [tm_meta()],              %% associated meta data
          state      :: given | blocked | failed  %% result of last request
        }).
-type tm_stat_live() :: #tm_stat_live{}.

%% Results of a "head" or "tail", per token
-record(tm_stat_hist,
        {
          limit   :: tm_count(),       %% maximum available, defined by token rate during interval
          refills :: tm_count(),       %% number of times this token was refilled during interval
          given   :: tm_count(),       %% number of this token type given in interval
          blocked :: tm_count()        %% number of blocked processes waiting for a token
        }).
-type tm_stat_hist() :: #tm_stat_hist{}.
-define(DEFAULT_TM_STAT_HIST,
        #tm_stat_hist{limit=0, refills=0, given=0, blocked=0}).

-define(DEFAULT_TM_SAMPLE_WINDOW, 60).    %% in seconds
-define(DEFAULT_TM_OUTPUT_SAMPLES, 20).   %% default number of sample windows displayed
-define(DEFAULT_TM_KEPT_SAMPLES, 10000).  %% number of history samples to keep
