-type sender_type() :: fsm | server | raw.
-type sender() :: {sender_type(), reference() | tuple(), pid()} |
                  %% TODO: Double-check that these special cases are kosher
                  {server, undefined, undefined} | % special case in
                                                   % riak_core_vnode_master.erl
                  {fsm, undefined, pid()} |        % special case in
                                                   % riak_kv_util:make_request/2.erl
                  ignore.
-type partition() :: chash:index_as_int().
-type vnode_req() :: term().
-type keyspaces() :: [{partition(), [partition()]}].

-record(riak_vnode_req_v1, {
          index :: partition() | undefined,
          sender=ignore :: sender(),
          request :: vnode_req()}).

-record(riak_coverage_req_v1, {
          index :: partition(),
          keyspaces :: keyspaces(),
          sender=ignore :: sender(),
          request :: vnode_req()}).

-record(riak_core_fold_req_v1, {
          foldfun :: fun(),
          acc0 :: term()}).
-record(riak_core_fold_req_v2, {
          foldfun :: fun(),
          acc0 :: term(),
          forwardable :: boolean(),
          opts = [] :: list()}).

-define(VNODE_REQ, #riak_vnode_req_v1).
-define(COVERAGE_REQ, #riak_coverage_req_v1).
-define(FOLD_REQ, #riak_core_fold_req_v2).
-define(KV_VNODE_LOCK(Idx), {vnode_lock, Idx}).

-type handoff_dest() :: {riak_core_handoff_manager:ho_type(), {partition(), node()}}.

%% An integer, and the number of bits to shift it left to treat it as
%% a mask in the 2^160 key space
%%
%% For a more thorough explanation of how these structures are used,
%% see `riak_core_coverage_plan'.
-type subpartition() :: { non_neg_integer(), pos_integer() }.

-record(vnode_coverage, {
          vnode_identifier = 0 :: non_neg_integer(),
          partition_filters = [] :: [non_neg_integer()],
          subpartition = undefined :: undefined | subpartition()
         }).

-type vnode_selector() :: all | allup.
-type vnode_coverage() :: #vnode_coverage{}.
