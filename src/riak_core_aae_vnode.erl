-module(riak_core_aae_vnode).

-export([behaviour_info/1]).

-spec behaviour_info(atom()) -> 'undefined' | [{atom(), arity()}].
behaviour_info(callbacks) ->
    [{aae_repair, 2},
     {hash_object, 2},
     {request_hashtree_pid, 1},
     {hashtree_pid, 1},
     {master, 0},
     {rehash, 3}];
behaviour_info(_Other) ->
    undefined.
