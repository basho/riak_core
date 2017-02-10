%%% @doc The riak_core_info_service is a way for dependencies of riak_core to sign
%%%      themselves up to receive messages from riak_core without having to generate
%%%      a cyclic call graph.
%%%      The dependency needs to have a way to be told where it should send requests
%%%      to get information out of riak_core. That is the Registration callback().
%%%      The Source callback() is a function inside riak_core that returns the
%%%      information that the dependency needs.
%%%      The Handler callback() is a function that will process the messages sent out
%%%      of the dependency when it wants information.
%%%      The dependency MUST send messages to the info service pid of the form 
%%%        {get, [term()]} 
%%%      
%%%      The information serivce originates from a need in eleveldb to know something
%%%      about bucket properties.
%%%      For that particular problem the callback()s would look like this:
%%%      Registration = {eleveldb, set_info_service_pid, []}
%%%      Source = {riak_core_bucket, get_bucket, []}
%%%      Handler = {eleveldb, process_bucket_props_request, []}
%%%      And the process_bucket_props_request function would look like this:
%%%      process_bucket_props_request({Mod, Fun, InitialArgs}=_Source, Name, Key]}) ->
%%%          Props = erlang:apply(Mod, Fun, InitialArgs++[Name]),
%%%          property_cache(Key, Props).
%%%
%%%      set_info_service_pid(_Pid) ->
%%%          erlang:nif_error({error, not_loaded}).
%%%
     



-module(riak_core_info_service).

-export([start_service/3]).


-type callback() :: {module(), FunName::atom(),InitialArgs::[term()]}.

-export_type([callback/0]).

-spec start_service(Registration::callback(), Source::callback(), Handler::callback()) -> 
                           ok |
                           {error, term()}.

start_service(Registration, Source, Handler) ->
    riak_core_info_service_sup:start_service(Registration, Source, Handler).


