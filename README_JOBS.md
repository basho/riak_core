# Riak Core Jobs

## NOTICE:

* **This feature is under active development**
* **EVERYTHING here is subject to change without notice**
* **Anything you read here may be a blatantly untrue**
* Don't like it? **Tell Me!** You know where to find me ...

### You have been warned!

## Contents

* [Overview](#overview)
  * [Notational Conventions](#notational-conventions)
  * [Terminology](#terminology)
    * [VNodeID](#vnodeid)
    * [vnode](#vnode)
    * [Unit of Work (UoW)](#unit-of-work-uow)
    * [Job](#job)
  * [Rationale](#rationale)
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
    * [Messages Messages Messages](#messages-messages-messages)
      * [Shutdown Handling](#shutdown-handling)
    * [Integration](#integration)
* [Comments Are Encouraged!](#comments-are-encouraged)

## Overview

Starting with version 2.3, Riak Core uses an entirely new Job Management API.
This document attempts to make sense of it.

### Notational Conventions

_\[Text Like This]_ describes features/functionality that are/is slated for inclusion, but possibly not in the first release.

_**Text Like This**_ introduces a term with an explicitly defined meaning.

> Text Like This provides commentary that (hopefully) clarifies something about the preceeding text.

### Terminology

To avoid confusion, it's worth noting a few key words and concepts.
I've reverted to my spec-writing persona for this section in an attemp to make it precise and normative, as the rest of this document may not make sense if you're not clear on how the following terms are used.

#### VNodeID

Within the scope of the Job Management API, a _**VNodeID**_ is the identifier of a logical information set, comprised of a _Type_ and a _Partition_ in a tuple of the form `{Type, Partition}`.
_Type_ conotes a particular behavior, while _Partition_ is treated as a distinct index into a list of information sets to which the _Type_ behavior applies.  
Two different _Partitions_ of the same _Type_ are assumed not to refer to the same information set, but beyond that _Type_ and _Partition_ are generally opaque.
There's more about the structure requirements and semantics of _**VNodeID**s_ in the section [About VNodeIDs](#about-vnodeids).

#### vnode

A _**vnode**_ is an Erlang process implementing the [`riak_core_vnode`](src/riak_core_vnode.erl) behavior.
Logically, a _**vnode**_ operates on an information set that can be identified by a _**VNodeID**_, but the _**vnode**_ may or may not identify itself as such.
Specifically, whatever identifiers may be used to specify a _**vnode**_ process are orthogonal to the _**VNodeID**_ that the Job Management API uses to identify the logical information set it operates upon.

The information set operated on by the _**vnode**_ process created by `riak_core_vnode:start_link(Mod, Index, ...)` is referred to in the Job Management API by the _**VNodeID**_ `{Mod, Index}`, but there does not necessarily have to exist a _**vnode**_ process for each _**VNodeID**_, either on the local Erlang node or anywhere in the distributed system of which it is a part.
> In Riak as it currently works there will be a _**vnode**_ process for each _**VNodeID**_, but the system would allow creation of job managers for _**VNodeID**s_ that don't correspond to any _**vnode**_.

#### Unit of Work (UoW)

A _**Unit of Work**_, also referred to as _**UoW**_, is a function, or list of functions, to be applied to the information set denoted by a particular _**VNodeID**_.
Because a _**vnode**_ correlates to a single _**VNodeID**_, and _**vnode**s_ are a central concept in `riak_core`, the job management system includes functionality for a _**UoW**_ to access the _**vnode**_ process matching its _**VNodeID**_, if one exists.

The `riak_core_job:work()` type represents a _**UoW**_ in the Job Management API. There is no status associated with a `riak_core_job:work()` object; it represents simply the operations to be executed, not where, how, or when.

#### Job

A _**Job**_ is a wrapper around a _**UoW**_ that correlates it across all of the informations sets (identified by _**VNodeID**s_) against which its _**UoW**_ is executed.
A _**Job**_ has a globally unique identifier and an assortment of attributes, such as a _Class_ that may be used for accepting/rejecting the _**Job**_, _\[ from whence it originated,]_ and status about whether and where it's been queued, executed, completed, killed, crashed, or cancelled.

The `riak_core_job:job()` type represents a _**Job**_ in the Job Management API.

### Rationale

The new API replaces the [Worker Pool](#worker-pool), and is designed to provide the following benefits:

* Visibility into what jobs are queued and running.
* Correlation of jobs across vnodes _\[and back to their originating client]_.
* Management of queued and running jobs.
  * _\[Dynamically]_ configurable filtering of jobs to be executed.
  * _\[Dynamically]_ configurable job concurency.
  * _\[Dynamically]_ configurable job queue limits.
* Every job runs in a pristine process environment.
  * The previous (deprecated) Worker Pool implementation, based on [poolboy](git://github.com/basho/poolboy), re-used existing processes for running UoWs.
Not only is this not _The Erlang Way_, but it leaves open the possibility that the process environment in which a UoW is running may have been poluted in some relevant way by a previous UoW that ran in it.

### API Status

#### Worker Pool

* This API is currently supported as a facade over the Job Management API.
* The existing API behavior is outwardly unchanged, but is **deprecated**.
* The [`riak_core_vnode_worker_pool`](src/riak_core_vnode_worker_pool.erl) and [`riak_core_vnode_worker`](src/riak_core_vnode_worker_pool.erl) modules in `riak_core`, and related `riak_xx_worker` modules implementing the `riak_core_vnode_worker` behavior in other components of Riak, _**will be removed**_ in version 3.0.

#### Job Management

* This API is introduced on the `feature-riak-2559` branch of affected Basho GitHub repositories.
* Until the API is merged onto the main 2.3 branch it should not be considered to be stabilized.
  * The `riak_core` job management implementation should be stable shortly after this file is visible, even if the API is not.
* Some API operations documented here _**may not**_ be available in the implementation yet, as the strategy is to document them first for review.

## How To Use It

The Job API is comprised of a lot of working components under the hood, but the API you use is pretty small. There are three basic concepts, the [Job](#more-about-jobs), the [Manager](#managing-jobs), and the [Service](#starting-and-stopping-vnode-job-managers).
> A _Manager_ is roughly analogous to the _Worker Pool_ it replaces.

A Job is an encapsulation of a unit of work, has a unique identifier, and can be run on one or a group of vnodes. When a job is run on multiple vnodes, all of the instances can be managed as a set.

A job is an object of type `riak_core_job:job()` and is created by invoking `riak_core_job:job([Properties])`. A minimal, though not very useful, job might be created as follows:

``` erlang
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

``` erlang
riak_core_job_mgr:submit(VNodeID, MyJob).
```

_\[To submit the job to a number of vnodes, you use the same function with a list of vnodes:]_

``` erlang
riak_core_job_mgr:submit([VNode1, ..., VNodeN], MyJob).
```

> A shortcut to providing a list of VNodeIDs is to provide just the _type_ of the vnodes you want the job to run on, in which case it will be forwarded to all running nodes of that type _in the local Erlang VM_. There's more on that [below](#about-vnodeids).

_\[In both cases, the return value of `submit(...)` tells you the disposition:]_

``` erlang
case riak_core_job_mgr:submit(TargetVNodes, MyJob) of
    ok ->
        % MyJob is queued or running (or already finished)
        ok;
    {error, Reason} ->
        % MyJob was not accepted, Reason tells us why
        oops
end.
```

_\[Note that submitting a job to multiple vnodes within an Erlang VM gives the appearance of being transactional, in that the job must be accepted by all nodes in order to run, and if any node rejects the job it doesn't run on any of them. However, the `Reason` reported will be the first rejection received and may not accurately reflect the responses you'd get from:]_

``` erlang
[riak_core_job_mgr:submit(N, MyJob) || N <- MyVNodes].
```

> The extent to which jobs can be transparently submitted across a cluster has not been addressed yet.

## Digging Deeper

### About VNodeIDs

The Job Management API uses the concept of a `VNodeID`, not the _Pid_ of a job manager (formerly a worker pool). Obviously, there are Erlang processes under the hood, but there are multiple interacting ones living within a supervision tree and no one pid is necessarily long-lived or the one you want for a particular operation.

A `VNodeID` is a unique identifier aligned with the `riak_core_vnode` model, specified (indirectly) as:

``` erlang
-type node_type()   :: atom().
-type node_id()     :: {node_type(), integer()}.
```

The elements are expected to be `{module(), non_neg_integer()}`, but aside from dialyzer warnings almost any 2-tuple whose first element is an atom will work (it _**is**_ matched in the code as a 2-tuple starting with an atom, but as of this writing the type of the second element is not checked, _though that could change!_) - a `VNodeID` of `{deep_thought, 42}` would work just fine, `{'Dent', "Arthur"}` _might_ work, and `{answer, 7.5, 42}` certainly would not.

VNodeIDs represent a grouping such that all vnodes with the same first element in their ID are assumed to be operating on the same type of vnode. This allows a single configuration to be used to start multiple VNodeIDs, and a job to be submitted to all vnodes of a type.

### Starting and Stopping VNode Job Managers

The `riak_core_job_svc` module exposes functions to start and stop per-vnode job managers and their related processes. The operations have different arguments to operate synchronously or asynchronously and with default, pre-existing, or specified configurations.

Starting and stopping vnode managers is accomplished with the following interfaces:

``` erlang
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

``` erlang
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

Unlike some of the other modules, the Job object is pretty stable at this point, and even has decent documentation, so refer to the \@doc comment for `job/1` in the [source](src/riak_core_job.erl) for details until I get around to polishing up the description here.

### Managing Jobs

The [introductory description](#how-to-use-it) tells you most of what you need to know about submitting jobs to be executed - it really is that simple - so for the time being I'll just provide the submission API specification in the `riak_core_job_mgr` module:

``` erlang
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

All running jobs on the vnode are killed, and their submitters are notified as described above ... _**unless**_ the supervisor is killed because its associated manager has died, in which case there's nobody to send the notifications.

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

#### Messages Messages Messages

The core messages within modules _should_ all be aligned (though they still need a thorough review by another set of eyes).

The basic pattern is that an API function _**F**_ sends a tuple whose first element is **'F'** and whose remaining elements are _**F**'s_ (non-routing) arguments. Those are the easy ones, though, and there are plenty where the pattern gets more complex.

The messages and parameters to callbacks need _at least_ better documentation, and I'm pretty sure the _job killed_ callback isn't fully plumbed through in the manager - more to review there.

##### Shutdown Handling

At present, there's pretty much no shutdown handling beyond the default behavior in a tree of supervisors and gen_servers.

Graceful shutdown would be a Good Thing, but making it all work while it's running is a higher priority.

#### Integration

Known packages that need to be fully switched over to the new API include:

* `riak_core` (obviously).
* `riak_kv`
* `riak_search`

_**PLEASE add what you know about to the list!**_

## Comments Are Encouraged!
