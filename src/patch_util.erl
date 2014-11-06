%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.

%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
-module(patch_util).
-export([loaded_taints/0, loaded_taints/1,
         code_path_taints/0, code_path_taints/1,
         dir_taints/2, path_taint/2]).

-export([incompatible_otp/1,
         incompatible_apps/1,
         obsolete_patches/2]).

%% To avoid blowing out the atom table, put a cap on the number of
%% patch names we can read from an obsoleted patches file.
-define(MAX_PATCHES, 1000).

%% We'll use -basho_patches, other users building riak_core
%% applications might use something else.
-define(DEFAULT_PRAGMA, basho_patches).

%%%%
%% Each patched beam should have a compile-time directive (termed a
%% module attribute or pragma) with a proplist including one or more
%% of the following keys:
%%

%% MANDATORY (although not really enforceable)

%%   name          Atom that uniquely describes this patch. If a patch is
%%                 compiled differently for multiple OTP/Riak
%%                 environments each should have a different name,
%%                 like disable_sslv3_r15 and disable_sslv3_r16.
%%
%%                 If multiple modules are updated for the same fix
%%                 the name should be the same in each.

%% OPTIONAL (but desirable)

%%   version       Patch version as an integer. This should be incremented
%%                 each time a patch is compiled/delivered to any
%%                 outside party. (Ideally it should be 1 for all
%%                 patches, but computers happen.)
%%
%%                 If missing, assumed to be 1.

%%   description   Something to make it trivial to discern the purpose of
%%                 this patch

%%   tickets       A list of ticket identifiers with more detail

%%   otp_target    An OTP release identifier appropriate to this patch

%%   app_target    A list of module and version tuples this patch should
%%                 work with

%% The version/release identifiers can either be a string or a list of
%% strings.
%%
%% Versions are matched purely by string comparison.
%%
%% For example, if a patch indicates it applies to {riak_core, "2."},
%% if riak_core is at version 2.anything, the patch will be considered
%% compatible. If the patch should apply to riak_core 2.0.x and 2.1.x
%% but not 2.2, {riak_core, ["2.0", "2.1"]} can be used.
%%
%% The otp_target and app_target fields imply some amount of
%% foreknowledge as to the length of time a patch will be
%% necessary.
%%
%% A patch which lacks version compatibility specifications will not
%% be reported by the incompatible_* functions.

%% Desirable enhancement: identification of patches in a specified
%% folder that lack these attributes.

%%%%
%% Example invocation from a 2.0 (thus R16) environment:
%%
%% (dev1@127.0.0.1)36> patch_checker:incompatible_otp(patch_checker:loaded_taints(basho_patches)).
%% [{dummy,[{name, basho_dummy_patch},
%%          {version, 1},
%%          {description,"Dummy module"},
%%          {tickets,["basho/riak_ee#123","JIRA-1234"]},
%%          {otp_target,"R15"},
%%          {app_target,[{riak_core,"2.0"},{yokozuna,["4","3.0"]}]}],
%%         "/Users/John/rt/current/dev/dev1/bin/../lib/basho-patches/dummy.beam"}]
%%
%% This module would also be returned by incompatible_apps since
%% Yokozuna is currently on version 2.0.x.


%%%%
%% For documentation purposes, if we were to turn the proplist of
%% metadata into a record, this is what it would look like.
%% -record(metadata,
%%         {
%%           name :: atom(),
%%           version :: pos_integer(),
%%           description :: string(),
%%           tickets :: list(string()),
%%           otp_target :: string() | list(string()),
%%           app_target :: module_version() | list(module_version())
%%         }).

-type fspath() :: file:filename().
-type metadata() :: list(tuple()).
-type patch_data() :: {module(), metadata(), fspath()}.
-type pragma() :: atom().

%% @doc Describe patched beams (with the appropriate metadata compiled
%% in) that are loaded into memory.
-spec loaded_taints() -> list(patch_data()).
loaded_taints() ->
    loaded_taints(?DEFAULT_PRAGMA).

-spec loaded_taints(pragma()) -> list(patch_data()).
loaded_taints(Pragma) ->
    lists:flatten([path_taint(Pragma, Path) || {_M, Path} <- code:all_loaded()]).

%% @doc Describe patched beams (with the appropriate metadata compiled
%% in) that are available in the running VM's code path.
-spec code_path_taints() -> list(list(patch_data())).
code_path_taints() ->
    code_path_taints(?DEFAULT_PRAGMA).

-spec code_path_taints(pragma()) -> list(list(patch_data())).
code_path_taints(Pragma) ->
    Paths = code:get_path(),
    lists:flatten([dir_taints(Pragma, Dir) || Dir <- Paths]).

%% @doc Describe patched beams (with the appropriate metadata compiled
%% in) that are available in the specified directory.
-spec dir_taints(pragma(), fspath()) -> list(list(patch_data())).
dir_taints(Pragma, Dir) ->
    [path_taint(Pragma, Path) || Path <- filelib:wildcard(Dir++"/*"++code:objfile_extension())].

%% @doc Describe patched beam (supplied as argument) if the
%% appropriate metadata is compiled in.
-spec path_taint(pragma(), fspath()) -> list(patch_data()).
path_taint(Pragma, Path) ->
    case patches_pragma(Pragma, Path) of
        [] ->
            [];
        Why ->
            M = list_to_atom(filename:basename(Path, code:objfile_extension())),
            [{M, Why, Path}]
    end.

%% -spec patches_pragma(pragma(), fspath()) -> metadata().
patches_pragma(Pragma, Path) ->
    try
        {ok, _M, Info} = beam_lib:all_chunks(Path),
        Attrs = binary_to_term(proplists:get_value("Attr", Info)),
        proplists:get_value(Pragma, Attrs, [])
    catch
        _:_Why ->
            %% io:format("~p: all_chunks failed : ~p", [Path, Why]),
            []
    end.

%% @doc Given a list of patches, return those that are (per the
%% compiled-in metadata) incompatible with the running VM.
-spec incompatible_otp(list(patch_data())) -> list(patch_data()).
incompatible_otp(Patches) ->
    OTPVsn = erlang:system_info(otp_release),
    lists:filter(fun({_Module, PatchMetadata, _Path}) ->
                         version_incompatible(proplists:get_value(otp_target, PatchMetadata, ""), OTPVsn) end,
                 Patches).

app_vsn(Module) ->
    {Module, _Label, Version} = lists:keyfind(Module,1,application:loaded_applications()),
    Version.

%% @doc Given a list of patches, return those that are (per the
%% compiled-in metadata) incompatible with the applications running in
%% the environment.
-spec incompatible_apps(list(patch_data())) -> list(patch_data()).
incompatible_apps(Patches) ->
    lists:filter(fun({_Module, PatchMetadata, _Path}) ->
                         case proplists:get_value(app_target, PatchMetadata, []) of
                             [] ->
                                 false;
                             {Module, Version} ->
                                 version_incompatible(Version, app_vsn(Module));
                             List ->
                                 lists:filter(fun({Module, Version}) ->
                                                      version_incompatible(Version, app_vsn(Module)) end,
                                              List) =/= []
                         end end,
                         Patches).


version_incompatible([], _Found) ->
    false;
version_incompatible(Targets, Found) when is_list(hd(Targets)) ->
    lists:filter(fun(Vsn) -> version_incompatible(Vsn, Found) end,
                 Targets) =:= Targets;
version_incompatible(Target, Found) ->
    string:sub_string(Found, 1, length(Target)) =/= Target.


%% Each release of Riak should come with a plain text file with a list
%% of obsoleted patches. Each line should be an atom (patch name).

%% @doc Given a list of obsoleted patches and a list of identified
%% patches in the environment, return the intersection.
-spec obsolete_patches(fspath() | list(atom()), list(patch_data())) -> list(patch_data()).
obsolete_patches([], Patches) -> %% Make sure hd(Names) in the next guard doesn't fail
    Patches;
obsolete_patches(Names, Patches) when is_atom(hd(Names)) ->
    FixedIssues = ordsets:from_list(Names),
    lists:filter(fun({_Module, PatchMetadata, _Path}) ->
                         ordsets:is_element(proplists:get_value(name, PatchMetadata), FixedIssues)
                 end,
                 Patches);
obsolete_patches(File, Patches) ->
    obsolete_patches(parse_obsoleted_file(File), Patches).

parse_obsoleted_file(File) when is_list(File) ->
    case file:open(File, [read, {encoding, utf8}]) of
        {error, _} ->
            io:format("Patch checker: file ~p could not be opened~n", [File]),
            [];
        {ok, Fh} ->
            Names = parse_obsoleted_file(Fh),
            _ = file:close(Fh),
            Names
    end;
parse_obsoleted_file(Fh) ->
    %% Let's make sure we don't blow up our atom table, set an
    %% arbitrary limit of MAX_PATCHES patch names.
    {ok, CommentsRE} = re:compile("[;%].*"),
    grab_patch_names(Fh, file:read_line(Fh), [], CommentsRE, ?MAX_PATCHES).

grab_patch_names(_Fh, eof, Accum, _RE, _Max) ->
    Accum;
grab_patch_names(_Fh, {error, _Reason}, Accum, _RE, _Max) ->
    Accum;
grab_patch_names(_Fh, {ok, Data}, Accum, RE, Max) when Max =< 0 ->
    Accum ++ patch_name_from_line(Data, RE);
grab_patch_names(Fh, {ok, Data}, Accum, RE, Max) ->
    grab_patch_names(Fh, file:read_line(Fh),
                     Accum ++ patch_name_from_line(Data, RE), RE, Max-1).

%% We want a single patch per line but we'll allow for someone who
%% can't read
patch_name_from_line(Line, CommentsRE) ->
    atomify(string:tokens(re:replace(Line, CommentsRE, "", [{return, list}]), ",;. \t\r\n")).

%% Unclear how the utf8 atom support 18.0 will work; will
%% list_to_atom() be the appropriate mechanism for this?
atomify(List) ->
    lists:map(fun(Name) -> list_to_atom(Name) end, List).
