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
    * [Job Manager](#job-manager)
    * [Unit of Work (UoW)](#unit-of-work-uow)
    * [Job](#job)
  * [Rationale](#rationale)
  * [API Status](#api-status)
    * [Worker Pool](#worker-pool)
    * [Job Management](#job-management)
* [How To Use It](#how-to-use-it)
  * [Submitting Jobs](#submitting-jobs)
* [The Supervision Tree](#the-supervision-tree)
  * [Managing All The Things](#managing-all-the-things)
    * [Global](#global)
      * [`riak_core_app`](#riak_core_app)
    * [Per VNode](#per-vnode)
      * [`riak_core_vnode_worker_pool` _(deprecated)_](#riak_core_vnode_worker_pool-_deprecated_)
* [More to Come](#more-to-come)
  * [Known Remaining Work](#known-remaining-work)
    * [Tests](#tests)
    * [Integration](#integration)
* [Comments Are Encouraged!](#comments-are-encouraged)

## Overview

Starting with version 2.3 (or 2.4?), Riak Core uses an entirely new Job Management API.
This document attempts to make sense of it.

### Notational Conventions

_\[Text Like This]_ describes features/functionality that are/is slated for inclusion, but possibly not in the first release.

_**Text Like This**_ introduces a term with an explicitly defined meaning.

> Text Like This provides commentary that (hopefully) clarifies something about the preceeding text.

### Terminology

To avoid confusion, it's worth noting a few key words and concepts.
I've reverted to my spec-writing persona for this section in an attemp to make it precise and normative, as the rest of this document may not make sense if you're not clear on how the following terms are used.
These descriptions are not necessarily applicable outside the Job Management API - a reasonable attempt has been made not to overload terms in the English or Erlang languages, or the concepts embodied in Riak, but there is clearly some overlap - so think within the namespace.

#### Job Manager

The _**Job Manager**_ replaces the per-[vnode](#vnode) [worker pools](#worker-pool) with a single (conceptual) process within the scope of a single Erlang VM.
This provides a single point where the total number of asynchronous jobs to be allowed/denied, prioritized, run concurently, queued, etc can be managed.
> Provisions are made to allow the appearance of distinct behavior configuration per vnode.

#### Unit of Work (UoW)

A _**Unit of Work**_, also referred to as _**UoW**_, is a function, or list of functions, to be executed asynchronously within the Job Management system.

The `riak_core_job:work()` type represents a _**UoW**_ in the Job Management API. There is no status associated with a `riak_core_job:work()` object; it represents simply the operations to be executed, not where, how, or when.

#### Job

A _**Job**_ is a wrapper around a _**UoW**_ that correlates it across all of the physical or virtual nodes on which it is to be run.
A _**Job**_ has a globally unique identifier and an assortment of attributes, such as a _Class_ that may be used for accepting/rejecting the _**Job**_, _\[from whence it originated]_, and status about whether _\[and where]_ it's been queued, executed, completed, killed, crashed, or cancelled.

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
* Because it's now a facade over the new system, it requires that the top-level Jobs processes are running. The `riak_core_app` takes care of this, but if it's not running the `riak_core_job_sup`, `riak_core_job_service`, and `riak_core_job_manager` processes need to be started _(in that order!)_ separately before creating a worker pool.
* The [`riak_core_vnode_worker_pool`](src/riak_core_vnode_worker_pool.erl) and [`riak_core_vnode_worker`](src/riak_core_vnode_worker_pool.erl) modules in `riak_core`, and related `riak_xx_worker` modules implementing the `riak_core_vnode_worker` behavior in other components of Riak, _**will be removed**_ in version 3.0.

#### Job Management

* This API is introduced on the `feature/riak-2559/...` branches of affected Basho GitHub repositories.
* Until the API is merged onto the main 2.3 branch it should not be considered to be stabilized.
  * The `riak_core` job management implementation is stable as of this writing, though the API is subject to change.
* Some API operations documented here _**may not**_ be available in the implementation yet, as the strategy is to document them first for review.

## How To Use It

The Job API is comprised of a lot of working components under the hood, but the API you use is pretty small. There are two basic concepts: the Job (module `riak_core_job`), and the Manager (module `riak_core_job_manager`).

Generating EDoc documentation for `riak_core` is strongly recommended before using the Jobs API. The above interfaces are fully documented.

### Submitting Jobs

A Job is created by invoking `riak_core_job:job([Properties])`, specified as:

``` erlang
-module(riak_core_job).

-spec job([{atom(), term()}]) -> job().
```

Refer to the documentation for `job/1` in [source](src/riak_core_job.erl) for details.

A minimal, though not very useful, job might be created as follows:

``` erlang
MyMain = fun(MgrKey) ->
    ThisJob = riak_core_job_manager:running_job(MgrKey),
    io:format("Job ~p running under manager ~p in process ~p~n",
        [riak_core_job:global_id(ThisJob), MgrKey, erlang:self()])
end,
MyJob = riak_core_job:job([
    {work,  riak_core_job:work([
        {main,  {MyMain, []}
    ])}
]).
```

Like `job/1`, `work/1` takes a list of properties, documented in `riak_core_job` [source](src/riak_core_job.erl).

To submit the Job, you just give it to the Manager:

``` erlang
riak_core_job_manager:submit(ScopeID, MyJob).
```

## The Supervision Tree

The Jobs supervision tree lives under the `riak_core_sup` tree, started by `riak_core_app`.

### Managing All The Things

#### Global
##### `riak_core_app`

```
  +-------------------------------------------------------------+
  |            Riak Core Application:  riak_core_sup            |
  +-------------------------------------------------------------+
     /        /               |                  \           \
   ...       /                |                   \          ...
            /                 |                    \
           /                  |                     \
          /                   |                      \
  +--------------+    +----------------+    +-------------------+
  | Jobs Manager |<-->| Runner Service |<-->| Runner Supervisor |  
  +--------------+    +----------------+    +-------------------+
                                                /       \
                                               /         \
                                              /           \
                                        +---------+   +---------+
                                        | Runner1 |...| RunnerN |
                                        +---------+   +---------+
```

All API requests are serviced by the `Jobs Manager`.

#### Per VNode
##### `riak_core_vnode_worker_pool` _(deprecated)_

```
  +--------------------------+                +---------+
  | VNode 1: riak_core_vnode |       ...      | VNode N |
  +--------------------------+                +---------+
                |                                  |
                |                                  |
                |                                  |
  +---------------+    +--------------+    +---------------+
  | Worker Pool 1 |--->| Jobs Manager |<---| Worker Pool N |
  +---------------+    +--------------+    +---------------+
```

In the above, the `Jobs Manager` is the global singleton started by the `riak_core` application, and the `Worker Pool` processes are façades that map the deprecated `riak_core_vnode_worker_pool` API onto the new Jobs API.
> Note that the Jobs Manager treats the Worker Pools like any other job submission client, hence the unidirectional communication.

## More to Come

### Known Remaining Work

#### Tests

There are some tests for basic functionality, but more would be good - wanna write 'em?

Refer to the [riak_core_jobs_tests](test/riak_core_jobs_tests.erl) module to see how to set up and clean up the necessary environment for eunit tests.

#### Integration

Known packages that need to be fully switched over to the new API include:

* `riak_core` (obviously).
* `riak_kv`
* `riak_search`

_**PLEASE add what you know about to the list!**_

## Comments Are Encouraged!
