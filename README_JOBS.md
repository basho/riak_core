# Riak Core Jobs

## NOTICE:

* **This feature is under active development**
* **EVERYTHING here is subject to change without notice**
* **Anything you read here may be a blatantly untrue**
* Don't like it? **Tell Me!** You know where to find me ...

### You have been warned!

## Contents

* [Overview](#overview)
  * [API Status](#api-status)
    * [Worker Pool](#worker-pool)
    * [Job Management](#job-management)
* [How To Use It](#how-to-use-it)
* [Digging Deeper](#digging-deeper)
  * [About VNodeIDs](#about-vnodeids)
  * [Starting and Stopping VNode Job Managers](#starting-and-stopping-vnode-job-managers)
  * [More About Jobs](#more-about-jobs)
  * [Managing Jobs](#managing-jobs)
* [The Supervision Tree](#the-supervision-tree)
  * [Managing All The Things](#managing-all-the-things)
    * [Global:](#global)
    * [Per VNode:](#per-vnode)
  * [Effects of Process Exits](#effects-of-process-exits)
    * [Runner](#runner)
    * [Work Supervisor](#work-supervisor)
    * [Manager](#manager)
    * [VNode Supervisor](#vnode-supervisor)
    * [Service](#service)
    * [Jobs Supervisor](#jobs-supervisor)
* [More to Come](#more-to-come)
  * [Known Remaining Work](#known-remaining-work)
    * [Tests](#tests)
    * [Shutdown Handling](#shutdown-handling)
    * [Integration](#integration)
* [Comments Are Encouraged!](#comments-are-encouraged)

## Overview

Starting with version 2.3, Riak Core uses an entirely new Job Management API designed to provide the following benefits:

* Visibility into what jobs are queued and running.
* Filtering of jobs to be executed.
* Management of queued and running jobs.
* Correlation of jobs across vnodes.
* Every job runs in a pristine process environment.

### API Status

#### Worker Pool

* This API is currently supported as a facade over the Job Management API.
* The existing API behavior is outwardly unchanged, but is **deprecated**.
* The [`riak_core_vnode_worker_pool`](src/riak_core_vnode_worker_pool.erl) and [`riak_core_vnode_worker`](src/riak_core_vnode_worker_pool.erl) modules in `riak_core`, and related `riak_xx_worker` modules implementing the `riak_core_vnode_worker` behavior in other components of Riak, ***will be removed*** in version 3.0.

#### Job Management

* This API is introduced on the `feature-riak-2559` branch of affected Basho GitHub repositories.
* Until the API is merged onto the main 2.3 branch it should not be considered to be stabilized.
  * The `riak_core` job management implementation should be stable shortly after this file is visible, even if the API is not.
* Some API operations documented here ***may not*** be available in the implementation yet, as the strategy is to document them first for review.

## How To Use It

The Job API is comprised of a lot of working components under the hood, but the API you use is pretty small. There are three basic concepts, the [Job](#more-about-jobs), the [Manager](#managing-jobs), and the [Service](#starting-and-stopping-vnode-job-managers).
> A _Manager_ is roughly analogous to the _Worker Pool_ it replaces.

A Job is an encapsulation of a unit of work, has a unique identifier, and can be run on one or a group of vnodes. When a job is run on multiple vnodes, all of the instances can be managed as a set.

A job is an object of type `riak_core_job:job()` and is created by invoking `riak_core_job:job([Properties])`. A minimal, though not very useful, job might be created as follows:

```
MyJob = riak_core_job:job([
    {work,  riak_core_job:work([
        {run,   {fun({VNodeID, Manager}) ->
            ThisJob = riak_core_job_mgr:job(Manager),
            io:format("Job ~p running on vnode ~p in process ~p~n",
                [riak_core_job:get(gid, ThisJob), VNodeID, erlang:self()])
        end, []}
    ])}
]).
```

To submit the job, you just tell it where to run:

```
riak_core_job_mgr:submit(VNodeID, MyJob).
```

To submit the job to a number of vnodes, you use the same function with a list of vnodes:

```
riak_core_job_mgr:submit([VNode1, ..., VNodeN], MyJob).
```

> A shortcut to providing a list of VNodeIDs is to provide just the _type_ of the vnodes you want the job to run on, in which case it will be forwarded to all running nodes of that type _in the local Erlang VM_. There's more on that [below](#about-vnodeids).

In both cases, the return value of `submit(...)` tells you the disposition:

```
case riak_core_job_mgr:submit(TargetVNodes, MyJob) of
    ok ->
        % MyJob is queued or running (or already finished)
        ok;
    {error, Reason} ->
        % MyJob was not accepted, Reason tells us why
        oops
end.
```

Note that submitting a job to multiple vnodes within an Erlang VM gives the appearance of being transactional, in that the job must be accepted by all nodes in order to run, and if any node rejects the job it doesn't run on any of them. However, the `Reason` reported will be the first rejection received and may not accurately reflect the responses you'd get from

```
[riak_core_job_mgr:submit(N, MyJob) || N <- MyVNodes].
```

> The extent to which jobs can be transparently submitted across a cluster has not been addressed yet.

## Digging Deeper

### About VNodeIDs

The Job Management API uses the concept of a `VNodeID`, not the _Pid_ of a job manager (formerly a worker pool). Obviously, there are Erlang processes under the hood, but there are multiple interacting ones living within a supervision tree and no one pid is necessarily long-lived or the one you want for a particular operation.

A `VNodeID` is a unique identifier aligned with the `riak_core_vnode` model, specified (indirectly) as:

```
-type node_type()   :: atom().
-type node_id()     :: {node_type(), integer()}.
```

The elements are expected to be `{module(), non_neg_integer()}`, but aside from dialyzer warnings almost any 2-tuple whose first element is an atom will work (it ***is*** matched in the code as a 2-tuple starting with an atom, but as of this writing the type of the second element is not checked, _though that could change!_) - a `VNodeID` of `{deep_thought, 42}` would work just fine, `{'Dent', "Arthur"}` _might_ work, and `{answer, 7.5, 42}` certainly would not.

VNodeIDs represent a grouping such that all vnodes with the same first element in their ID are assumed to be operating on the same type of vnode. This allows a single configuration to be used to start multiple VNodeIDs, and a job to be submitted to all vnodes of a type.

### Starting and Stopping VNode Job Managers

The `riak_core_job_svc` module exposes functions to start and stop per-vnode job managers and their related processes. The operations have different arguments to operate synchronously or asynchronously and with default, pre-existing, or specified configurations.

Starting and stopping vnode managers is accomplished with the following interfaces:

```
-type timeout() :: non_neg_integer() | 'infinity'.
-type config()  :: [
    {node_job_accept, {module(), atom(), [term()]} | {fun(), [term()]}}
  | {node_job_concur, pos_integer()}
  | {node_job_queue,  non_neg_integer()}
].

-spec start_node(VNodeID) -> Result when
        VNodeID :: node_id(),
        Result  :: ok | {error, term()}.

-spec start_node(VNodeID, Config) -> Result when
        VNodeID :: node_id(),
        Config  :: config(),
        Result  :: ok | {error, term()}.

-spec stop_node(VNodeID) -> Result when
        VNodeID :: node_id(),
        Result  :: ok | {error, term()}.

-spec stop_node(VNodeID, Timeout) -> Result when
        VNodeID :: node_id(),
        Timeout :: timeout(),
        Result  :: ok | {error, term()}.
```
> Until the API is finalized, configuration properties are only documented in [`riak_core_job_mgr.erl`](src/riak_core_job_mgr.erl).

_**Note:** Starting a manager without a configuration **is not** the same as starting it with `Config = []` - the former tries to find and use a configuration specified when starting a previous manager of the same type, while the latter uses the internal default configuration._


Additionally, some of the following interfaces _may_ be included if they are determined to have value:

```
-spec start_nodes(VNodes) -> Result when
        VNodes  :: node_type() | [node_id()],
        Result  :: ok | {error, term()}.

-spec start_nodes(VNodes, Config) -> Result when
        VNodes  :: node_type() | [node_id()],
        Config  :: config(),
        Result  :: ok | {error, term()}.

-spec stop_nodes(VNodes) -> Result when
        VNodes  :: node_type() | [node_id()],
        Result  :: ok | {error, term()}.

-spec stop_nodes(VNodes, Timeout) -> Result when
        VNodes  :: node_type() | [node_id()],
        Timeout :: timeout(),
        Result  :: ok | {error, term()}.
```
> It's not clear whether there are suitable use cases for starting and stopping job managers separately from their controlling vnodes.

### More About Jobs

Unlike some of the other modules, the Job object is pretty stable at this point, and even has decent documentation, so refer to the @doc comment for `job/1` in the [source](src/riak_core_job.erl) for details until I get around to polishing up the description here.

### Managing Jobs

The [introductory description](#how-to-use-it) tells you most of what you need to know about submitting jobs to be executed - it really is that simple - so for the time being I'll just provide the submission API specification in the `riak_core_job_mgr` module:

```
-spec submit(Where, Job) -> Result when
        Where   :: node_type() | node_id() | [node_id()],
        Result  :: ok | {error, Reason},
        Reason  :: job_queue_full | job_rejected | vnode_shutdown | term().
```
> There are, as well, a bunch of possible job validation errors, as the job's callbacks aren't fully checked until it arrives at the node on which it's going to be run. For instance, we don't check for a specified exported function when the job's created because it's not an error to specify a {M, F, A} that's not present at that time, it only becomes an error if it's not present on the node that will run it.

When the rest of the job management operations are stabilized, we'll expand upon them here.

## The Supervision Tree

### Managing All The Things

#### Global:

```
             +----------------+
             | riak_core_sup  |
             +----------------+
               /     |      \
             ...     |      ...
                     |
            +------------------+
            | Jobs Supervisor  |
            +------------------+
             /       |        \
            /        |         \
           /         |          \
  +---------+   +--------+     +--------+
  | Service |   | VNode1 | ... | VNodeN |  
  +---------+   +--------+     +--------+
```

#### Per VNode:

```
      +------------------+
      | VNode Supervisor |
      +------------------+
          /           \
         /             \
        /               \
  +---------+   +-----------------+
  | Manager |   | Work Supervisor |
  +---------+   +-----------------+
                    /         \
                   /           \
                  /             \
            +---------+     +---------+
            | Runner1 | ... | RunnerN |
            +---------+     +---------+
```

In the above, the `Jobs Supervisor` is a singleton started by the `riak_core` application, and everything but the `Runner` processes is restarted automatically if they crash for any reason. The `Service` process acts primarily to serialize and coordinate starting and stopping `VNode Supervisors` and acts as the name registry to map VNodeIDs to their servicing processes.

### Effects of Process Exits

The various supervisors do nothing but watch their child processes, so they shouldn't go down unexpectedly. Nevertheless, the effect if they do is noted for completeness.

#### Runner

These processes are created individually to run each unit of work. On a clean exit, no extrenal action is taken, as it is presumed that if the submitter wants to know when the job finishes, it will include a suitable callback in the job's unit of work.

If the unit of work crashes before the process has notified the manager that it's done and the job includes a `killed` callback or a `from` attribute that is not `ignore`, the submitter is notified of the crash.

#### Work Supervisor

All running jobs on the vnode are killed, and their submitters are notified as described above ... ***unless*** the supervisor is killed because its associated manager has died, in which case there's nobody to send the notifications.

If the manager is still running, its queued work will be dispatched when the supervisor is automatically restarted.

#### Manager

Without a doubt, this is the process with the most to lose if a nasty job were to take it down, so it's designed to be as tolerant of such ugliness as it can be. Because it monitors all of its running work, loss of this process triggers shutdown of its work supervisor, and by extension all running jobs under it.

It will be restarted automatically with its original configuration if it crashes, but all queued and running work on the vnode would be lost.

#### VNode Supervisor

Probably the simplest process in the tree, so it's highly unlikely to crash, but if it did the effect would be the same as a Manager crash. Like the manager, it will be restarted automatically with its original configuration if it crashes, but all queued and running work on the vnode would be lost.

#### Service

While this process maintains a lot of state, it's basically a cache so it's able to fully repopulate itself by crawling an existing supervision tree if it's automatically restarted by its controlling supervisor.

A restart _could_ cause an error to propagate out from an external manager operation, probably as a `noproc` error, but it's not clear that there's sufficient reason to protect against that, as it should be a pretty robust process.

#### Jobs Supervisor

It's all dead and gone, so sorry, sucks to be you. On the upside, if this process goes away it's probably because the `riak_core` application is itself restarting, or is so thoroughly hosed you're better off without it.

## More to Come

### Known Remaining Work

#### Tests

Yeah, tests would probably be good to have.  Wanna write 'em?

#### Shutdown Handling

At present, there's pretty much no shutdown handling beyond the default behavior in a tree of supervisors and gen_servers.

Graceful shutdown would be a Good Thing, but making it all work is a higher priority.

#### Integration

Known packages that need to be fully switched over to the new API include:

* `riak_core` (obviously).
* `riak_kv`
* `riak_search`

***PLEASE add what you know about to the list!***

## Comments Are Encouraged!

