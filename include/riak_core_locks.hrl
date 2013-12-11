%% Copyright(C), Basho Technologies, 2013
%%
%% @doc Definitions of shared locks/tokens for use with background manager.
%% See @link riak_core_background_mgr:get_lock/1
%%
%% @doc vnode_lock(Module, PartitionIndex) is a kv per-vnode lock, used possibly,
%% by AAE tree rebuilds, fullsync, and handoff.
-define(VNODE_LOCK(Mod, Idx), {vnode_lock, Mod, Idx}).
