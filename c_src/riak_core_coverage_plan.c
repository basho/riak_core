#define __STDC_FORMAT_MACROS
#include <erl_nif.h>
#include <string.h>
#include <stdio.h>
#include <inttypes.h>

#define OK 1
#define ERR 0
#define BASE 64
#define EMPTY_SET 0

struct state {
  unsigned int offset;
  unsigned int nval;
  uint64_t coverage[BASE];
};

typedef unsigned char vnode_t;
typedef struct state state_t;

static int load(ErlNifEnv* env, void** priv, ERL_NIF_TERM load_info)
{
    return 0;
}

/* Bitmask arithmetics */
static uint64_t intersect(const uint64_t bm1, const uint64_t bm2)
{
    return bm1 & bm2;
}

static uint64_t subtract(const uint64_t bm1, const uint64_t bm2)
{
    return (bm1 ^ bm2) & bm1;
}

static uint64_t singleton(vnode_t vnode)
{
    return (uint64_t) 1 << vnode;
}

static vnode_t vnode_by_tie_breaker(unsigned int tie_breaker, state_t *state)
{
    return (tie_breaker - state->offset) & (BASE - 1);
}

static unsigned int covers(uint64_t key_spaces,
			   vnode_t vnode,
			   state_t *state)
{
    int nval = state->nval;
    int pos;
    uint64_t flag;
    unsigned int cover_num = 0;

    while (nval > 0) {
	pos = vnode - nval;
	if (pos > 0)
	    flag = (uint64_t) 1 << pos;
	else
	    flag = (uint64_t) 1 << (BASE + pos);

	if (intersect(flag, key_spaces) != EMPTY_SET)
	    cover_num++;

	nval--;
    }

    return cover_num;
}

static void next_vnode(uint64_t key_spaces,
		       uint64_t available_key_spaces,
		       state_t *state,
		       vnode_t *max_vnode,
		       unsigned int *max_cover_num)
{
    vnode_t vnode;
    unsigned int cover_num;
    unsigned int tie_breaker = 0;
    *max_vnode = 0;
    *max_cover_num = 0;

    while (tie_breaker < BASE) {
	vnode = vnode_by_tie_breaker(tie_breaker, state);
	if (intersect(available_key_spaces, singleton(vnode)) != EMPTY_SET) {
	    cover_num = covers(subtract(key_spaces, state->coverage[vnode]), vnode, state);
	    if (cover_num == state->nval) {
		*max_vnode = vnode;
		*max_cover_num = cover_num;
		break;
	    } else if (cover_num > *max_cover_num) {
		*max_vnode = vnode;
		*max_cover_num = cover_num;
	    }
	};
	tie_breaker++;
    }
}

static uint64_t n_keyspaces(vnode_t vnode, state_t *state)
{
    int shift = vnode - state->nval;
    uint64_t bm = ((uint64_t) 1 << state->nval) - 1;
    if (shift > 0)
	return (bm << shift) | (bm >> (BASE - shift));
    else
	return (bm << (BASE+shift)) | (bm >> -shift);
}

static ERL_NIF_TERM bitmask_to_list(ErlNifEnv* env, uint64_t bm)
{
    ERL_NIF_TERM tail = enif_make_list(env, 0);
    int i;

    for (i=BASE-1; i>=0; i--) {
	if (bm & ((uint64_t) 1 << i)) {
	    tail = enif_make_list_cell(env, enif_make_uint(env, i), tail);
	}
    }
    
    return tail;
}

static ERL_NIF_TERM coverage_to_list(ErlNifEnv* env, uint64_t coverage[BASE])
{
    ERL_NIF_TERM tail = enif_make_list(env, 0);
    ERL_NIF_TERM tuple;
    
    int i;
    
    for (i = BASE-1; i >= 0; i--) {
	if (coverage[i] != 0) {
	    tuple = enif_make_tuple2(env, enif_make_uint(env, i),
				     bitmask_to_list(env, coverage[i]));
	    tail = enif_make_list_cell(env, tuple, tail);
	}
    }

    return tail;
}

static uint64_t find_coverage_vnodes(ErlNifEnv* env,
				     uint64_t key_spaces,
				     uint64_t available_key_spaces,
				     state_t *state)
{
    vnode_t vnode;
    unsigned int cover_num;
    uint64_t covers;

    while (1) {
	if (key_spaces == EMPTY_SET || available_key_spaces == EMPTY_SET) {
	    return (uint64_t) key_spaces;
	} else {
	    next_vnode(key_spaces, available_key_spaces, state, &vnode, &cover_num);
	    if (cover_num == 0) {
		/* out of vnodes */
		available_key_spaces = EMPTY_SET;
	    } else {
		covers = subtract(n_keyspaces(vnode, state), state->coverage[vnode]);
		available_key_spaces = subtract(available_key_spaces, singleton(vnode));
		state->coverage[vnode] |= (uint64_t) intersect(key_spaces, covers);
		key_spaces = subtract(key_spaces, covers);
	    }
	}
    }
}

static int list_to_bitmask(ErlNifEnv* env, ERL_NIF_TERM list, uint64_t *bm)
{
    ERL_NIF_TERM head, tail;
    unsigned int vnode;
    *bm = 0;

    while (enif_get_list_cell(env, list, &head, &tail)) {
	if (enif_get_uint(env, head, &vnode))
	    *bm = *bm | ((uint64_t) 1 << vnode);
	else
	    return ERR;
	list = tail;
    }

    return OK;
}

static ERL_NIF_TERM find_coverage(ErlNifEnv* env, int argc,
				  const ERL_NIF_TERM argv[])
{
    uint64_t unavailable_key_spaces;
    uint64_t all_key_spaces;
    uint64_t result_key_spaces;
    uint64_t vnodes;
    unsigned int pvc = 0;
    state_t state = {.coverage = {0}};
    
    if (argc == 4) {
	if (enif_get_uint(env, argv[0], &state.offset) &&
	    enif_get_uint(env, argv[1], &state.nval) &&
	    enif_is_list(env, argv[2]) &&
	    enif_get_uint(env, argv[3], &pvc) &&
	    list_to_bitmask(env, argv[2], &unavailable_key_spaces))
	    {
		all_key_spaces = UINT64_MAX;
		vnodes = subtract(all_key_spaces, unavailable_key_spaces);
		do {
		    result_key_spaces = find_coverage_vnodes(env, all_key_spaces,
							     vnodes, &state);
		    pvc--;
		} while (pvc > 0 && result_key_spaces == EMPTY_SET);
		if (result_key_spaces == EMPTY_SET)
		    return enif_make_tuple2(env, enif_make_atom(env, "ok"),
					    coverage_to_list(env, state.coverage));
		else
		    return enif_make_tuple3(env,
					    enif_make_atom(env,
							   "insufficient_vnodes_available"),
					    bitmask_to_list(env, result_key_spaces),
					    coverage_to_list(env, state.coverage));
	    } else {
	    return enif_make_badarg(env);
	}
    }

    return enif_make_badarg(env);
}

static ErlNifFunc nif_funcs[] =
    {
	{"find_coverage_fast", 4, find_coverage}
    };

ERL_NIF_INIT(riak_core_coverage_plan, nif_funcs, load, NULL, NULL, NULL)
