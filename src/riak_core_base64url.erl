%% -------------------------------------------------------------------
%%
%% Copyright (c) 2009-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
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
%%
%% -------------------------------------------------------------------

%% @doc base64url is a wrapper around the base64 module to produce
%%      base64-compatible encodings that are URL safe.
%%      The / character in normal base64 encoding is replaced with
%%      the _ character, and + is replaced with -.
%%      This replacement scheme is named "base64url" by
%%      http://en.wikipedia.org/wiki/Base64

-module(riak_core_base64url).

-export([decode/1,
         decode_to_string/1,
         encode/1,
         encode_to_string/1,
         mime_decode/1,
         mime_decode_to_string/1]).

-spec decode(iodata()) -> binary().
decode(Base64url) ->
    base64:decode(urldecode(Base64url)).

-spec decode_to_string(iodata()) -> string().
decode_to_string(Base64url) ->
    base64:decode_to_string(urldecode(Base64url)).

-spec mime_decode(iodata()) -> binary().
mime_decode(Base64url) ->
    base64:mime_decode(urldecode(Base64url)).

-spec mime_decode_to_string(iodata()) -> string().
mime_decode_to_string(Base64url) ->
    base64:mime_decode_to_string(urldecode(Base64url)).

-spec encode(iodata()) -> binary().
encode(Data) ->
    urlencode(base64:encode(Data)).

-spec encode_to_string(iodata()) -> string().
encode_to_string(Data) ->
    urlencode(base64:encode_to_string(Data)).

urlencode(Base64) when is_list(Base64) ->
    Padded = [urlencode_digit(D) || D <- Base64],
    string:strip(Padded, both, $=);
urlencode(Base64) when is_binary(Base64) ->
    Padded = << << (urlencode_digit(D)) >> || <<D>> <= Base64 >>,
    binary:replace(Padded, <<"=">>, <<"">>, [global]).

urldecode(Base64url) when is_list(Base64url) ->
    Prepad = [urldecode_digit(D) || D <- Base64url ],
    Padding = padding(Prepad),
    Prepad ++ Padding;
urldecode(Base64url) when is_binary(Base64url) ->
    Prepad = << << (urldecode_digit(D)) >> || <<D>> <= Base64url >>,
    Padding = padding(Prepad),
    <<Prepad/binary, Padding/binary>>.

padding(Base64) when is_binary(Base64) ->
   case byte_size(Base64) rem 4 of
        2 ->
            <<"==">>;
        3 ->
            <<"=">>;
        _ ->
            <<"">>
    end;
padding(Base64) when is_list(Base64) ->
    binary_to_list(padding(list_to_binary(Base64))).

urlencode_digit($/) -> $_;
urlencode_digit($+) -> $-;
urlencode_digit(D)  -> D.

urldecode_digit($_) -> $/;
urldecode_digit($-) -> $+;
urldecode_digit(D)  -> D.
