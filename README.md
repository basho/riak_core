# What is riak_core_ng?

The 'ng' version of riak core is a modernized version of riak core, it is entirely build with rebar3 using hex packages for all dependences, with more modern versions of some libraries. 99.9% of all credit goes to basho, very little extra work has happened here! The aim is to keep this up to date with all changes pushed to riak_core proper.

## Improvements and Additions

* R19 compatibility - This form works with R18 and R19 (lower versions should work but are not tested)
* AAE - this version includes the Active Anti Entropy code extracted from `riak_kv` and provides a new behavior called `riak_core_aae_vnode`.

# Riak Core

[![Build Status](https://secure.travis-ci.org/basho/riak_core.png)](http://travis-ci.org/basho/riak_core)

Riak Core is the distributed systems framework that forms the basis of
how [Riak](http://github.com/basho/riak) distributes data and scales.
More generally, it can be thought of as a toolkit for building
distributed, scalable, fault-tolerant applications.

For some introductory reading on Riak Core (that’s not pure code),
there’s an old but still valuable
[blog post on the Basho Blog](http://basho.com/where-to-start-with-riak-core/)
that’s well worth your time.

## Contributing

We love community code, bug fixes, and other forms of contribution. We
use GitHub Issues and Pull Requests for contributions to this and all
other code. To get started:

1. Fork this repository.
2. Clone your fork or add the remote if you already have a clone of
   the repository.
3. Create a topic branch for your change.
4. Make your change and commit. Use a clear and descriptive commit
   message, spanning multiple lines if detailed explanation is needed.
5. Push to your fork of the repository and then send a pull request.

6. A Riak committer will review your patch and merge it into the main
   repository or send you feedback.

## Issues, Questions, and Bugs

There are numerous ways to file issues or start conversations around
something Core related

* The
  [Riak Users List](http://lists.basho.com/mailman/listinfo/riak-users_lists.basho.com)
  is the main place for all discussion around Riak.
* There is a
  [Riak Core-specific mailing list](http://lists.basho.com/mailman/listinfo/riak-core_lists.basho.com)
  for issues and questions that pertain to Core but not Riak.
* #riak on Freenode is a very active channel and can get you some
   real-time help if a lot of instances.
* If you've found a bug in Riak Core,
  [file](https://github.com/basho/riak_core/issues) a clear, concise,
  explanatory issue against this repo.
