%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
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

-module(riak_core_console_table).

%% API
-export([print/2, print/3,
         create_table/2, autosize_create_table/2]).

-include("riak_core_status_types.hrl").

-define(MAX_LINE_LEN, 100).

-spec print(list(), list()) -> ok.
print(_Spec, []) ->
    ok;
%% Explict sizes were not given. This is called using the new status types.
print(Schema, Rows) when is_list(hd(Schema)) ->
    Table = autosize_create_table(Schema, Rows),
    io:format("~n~ts~n", [Table]);
print(Spec, Rows) ->
    Table = create_table(Spec, Rows),
    io:format("~n~ts~n", [Table]).

-spec print(list(), list(), list()) -> ok.
print(_Hdr, _Spec, []) ->
    ok;
print(Header, Spec, Rows) ->
    Table = create_table(Spec, Rows),
    io:format("~ts~n~n~ts~n", [Header, Table]).

-spec autosize_create_table([any()], [[any()]]) -> iolist().
autosize_create_table(Schema, Rows) ->
    BorderSize = 1 + length(hd(Rows)),
    MaxLineLen = case io:columns() of
	             %% Leaving an extra space seems to work better
	             {ok, N} -> N - 1; 
		     {error, enotsup} -> ?MAX_LINE_LEN
		 end,
    Sizes = get_field_widths(MaxLineLen - BorderSize, [Schema | Rows]),
    Spec = lists:zip(Schema, Sizes),
    create_table(Spec, Rows, MaxLineLen, []).
	            
-spec create_table(list(), list()) -> iolist().
create_table(Spec, Rows) ->
    Lengths = get_row_length(Spec, Rows),
    Length = lists:sum(Lengths)+2,
    AdjustedSpec = [{Field, NewLength} || {{Field, _DefaultLength}, NewLength}
                                          <- lists:zip(Spec, Lengths)],
    create_table(AdjustedSpec, Rows, Length, []).

-spec create_table(list(), list(), non_neg_integer(), iolist()) -> iolist().
create_table(Spec, Rows, Length, []) ->
    FirstThreeRows = [vertical_border(Spec), titles(Spec),
                      vertical_border(Spec)],
    create_table(Spec, Rows, Length, FirstThreeRows);
create_table(_Spec, [], _Length, IoList) when length(IoList) == 3 ->
    %% table had no rows, no final row needed
    lists:reverse(IoList);
create_table(Spec, [], _Length, IoList) ->
    BottomBorder = vertical_border(Spec),
    %% There are no more rows to print so return the table
    lists:reverse([BottomBorder | IoList]);
create_table(Spec, [Row | Rows], Length, IoList) ->
    create_table(Spec, Rows, Length, [row(Spec, Row) | IoList]).

-spec get_field_widths(pos_integer(), [term()]) ->  [pos_integer()].
get_field_widths(MaxLineLen, Rows) ->
    Widths = max_widths(Rows),
    resize_row(MaxLineLen, Widths).

-spec resize_row(pos_integer(), [pos_integer()]) -> [pos_integer()].
resize_row(MaxLength, Widths) ->
    Sum = lists:sum(Widths),
    case Sum > MaxLength of
	true ->
	    resize_items(Sum, MaxLength, Widths);
	false ->
	    Widths
    end.

-spec resize_items(pos_integer(), pos_integer(), [pos_integer()]) ->
    [pos_integer()].
resize_items(Sum, MaxLength, Widths) ->
    Diff = Sum - MaxLength,
    NumColumns = length(Widths),
    case NumColumns > Diff of
	true ->
	    Remaining = NumColumns - Diff ,
	    reduce_widths(1, Remaining, Widths);
	false ->
	    PerColumn = Diff div NumColumns + 1,
	    Remaining = Diff - NumColumns,
	    reduce_widths(PerColumn, Remaining, Widths)
    end.
-spec reduce_widths(pos_integer(), pos_integer(), [pos_integer()]) ->
    [pos_integer()].
reduce_widths(PerColumn, Total, Widths) ->
    %% Just subtract one character from each column until we run out.
    {_, NewWidths} = 
        lists:foldl(fun(Width, {Remaining, NewWidths}) ->
		        case Remaining of
			    0 ->
				{0, [Width | NewWidths]};
			    _ ->
				Rem = Remaining - PerColumn,
				{Rem, [Width - PerColumn | NewWidths]}
			end
		    end, {Total, []}, Widths),
    NewWidths.

get_row_length(Spec, Rows) ->
    Res = lists:foldl(fun({_Name, MinSize}, Total) ->
			Longest = find_longest_field(Rows, length(Total)+1),
                        Size = erlang:max(MinSize, Longest),
                        [Size | Total]
                end, [], Spec),
    lists:reverse(Res).

-spec find_longest_field(list(), pos_integer()) -> non_neg_integer().
find_longest_field(Rows, ColumnNo) ->
        lists:foldl(fun(Row, Longest) ->
		        erlang:max(Longest,
			          field_length(lists:nth(ColumnNo,Row)))
		    end, 0, Rows).

-spec max_widths([term()]) -> list(pos_integer()).
max_widths([Row]) ->
    field_lengths(Row);
max_widths([Row1 | Rest]) ->
    Row1Lengths = field_lengths(Row1),
    lists:foldl(fun(Row, Acc) ->
		    Lengths = field_lengths(Row),
		    [max(A, B) || {A, B} <- lists:zip(Lengths, Acc)]
		end, Row1Lengths, Rest).

-spec row(list(), list(string())) -> iolist().
row(Spec, Row0) ->
    %% handle multiline fields
    Rows = expand_row(Row0),
    [
     [ $| | lists:reverse(
              ["\n" | lists:foldl(fun({{_, Size}, Str}, Acc) ->
                                          [align(Str, Size) | Acc]
                                  end, [], lists:zip(Spec, Row))])] || Row <- Rows].

-spec titles(list()) -> iolist().
titles(Spec) ->
    [ $| | lists:reverse(
        ["\n" | lists:foldl(fun({Title, Size}, TitleRow) ->
                               [align(Title, Size) | TitleRow]
                            end, [], Spec)])].

-spec align(string(), non_neg_integer()) -> iolist().
align(undefined, Size) ->
    align("", Size);
align(Str, Size) when is_integer(Str) ->
    align(integer_to_list(Str), Size);
align(Str, Size) when is_binary(Str) ->
    align(unicode:characters_to_list(Str, utf8), Size);
align(Str, Size) when is_atom(Str) ->
    align(atom_to_list(Str), Size);
align(Str, Size) when is_list(Str), length(Str) >= Size ->
    Truncated = lists:sublist(Str, Size),
    Truncated ++ "|";
align(Str, Size) when is_list(Str) ->
    string:centre(Str, Size) ++ "|";
align(Term, Size) ->
    Str = lists:flatten(io_lib:format("~p", [Term])),
    align(Str, Size).

-spec vertical_border(list(tuple())) -> string().
vertical_border(Spec) ->
    lists:reverse([$\n, [[char_seq(Length, $-), $+] ||
                             {_Name, Length} <- Spec], $+]).

-spec char_seq(non_neg_integer(), char()) -> string().
char_seq(Length, Char) ->
    [Char || _ <- lists:seq(1, Length)].

field_lengths(Row) ->
    [field_length(Field) || Field <- Row].

field_length(Field) when is_atom(Field) ->
    field_length(atom_to_list(Field));
field_length(Field) when is_binary(Field) ->
    field_length(unicode:characters_to_list(Field, utf8));
field_length(Field) when is_list(Field) ->
    Lines = string:tokens(lists:flatten(Field), "\n"),
    lists:foldl(fun(Line, Longest) ->
                        erlang:max(Longest,
                                   length(Line))
                end, 0, Lines);
field_length(Field) ->
    field_length(io_lib:format("~p", [Field])).

expand_field(Field) when is_atom(Field) ->
    expand_field(atom_to_list(Field));
expand_field(Field) when is_binary(Field) ->
    expand_field(unicode:characters_to_list(Field, utf8));
expand_field(Field) when is_list(Field) ->
    string:tokens(lists:flatten(Field), "\n");
expand_field(Field) ->
    expand_field(io_lib:format("~p", [Field])).

expand_row(Row) ->
    {ExpandedRow, MaxHeight} = lists:foldl(fun(Field, {Fields, Max}) ->
                                                    EF = expand_field(Field),
                                                    {[EF|Fields], erlang:max(Max, length(EF))}
                                            end, {[], 0}, lists:reverse(Row)),
    PaddedRow = [pad_field(Field, MaxHeight) || Field <- ExpandedRow],
    [ [ lists:nth(N, Field) || Field <- PaddedRow]
      || N <- lists:seq(1, MaxHeight)].

pad_field(Field, MaxHeight) when length(Field) < MaxHeight ->
    Field ++ ["" || _ <- lists:seq(1, MaxHeight - length(Field))];
pad_field(Field, _MaxHeight) ->
    Field.
