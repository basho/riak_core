-type sender_type() :: fsm | server | raw.
-type sender() :: {sender_type(), reference(), pid()} |
                  %% TODO: Double-check that these special cases are kosher
                  {server, undefined, undefined} | % special case in
                                                   % riak_core_vnode_master.erl
                  {fsm, undefined, pid()} |        % special case in
                                                   % riak_kv_util:make_request/2.erl
                  ignore.
-type partition() :: non_neg_integer().
-type vnode_req() :: term().

-record(riak_vnode_req_v1, {
          index :: partition(),
          sender=ignore :: sender(),
          request :: vnode_req()}).

-record(riak_coverage_req_v1, {
          index :: partition(),
          keyspaces :: [{partition(), [partition()]}],
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
