-module(table).

%% API
-export([print/2,
         create_table/2]).

-spec print(list(), list()) -> ok.
print(Spec, Rows) ->
    Table = create_table(Spec, Rows),
    io:format("~s", [Table]).

-spec create_table(list(), list()) -> iolist().
create_table(Spec, Rows) ->
    Length = get_row_length(Spec),
    create_table(Spec, Rows, Length, []).

-spec create_table(list(), list(), non_neg_integer(), iolist()) -> iolist().
create_table(Spec, Rows, Length, []) ->
    FirstThreeRows = [vertical_border(Length), titles(Spec), vertical_border(Length)],
    create_table(Spec, Rows, Length, FirstThreeRows);
create_table(_Spec, [], Length, IoList) when length(IoList) =:= 2 ->
    BottomBorder = vertical_border(Length),
    %% There are no more rows to print so return the table
    lists:reverse([BottomBorder | IoList]);
create_table(_Spec, [], _Length, IoList) ->
    lists:reverse(IoList);
create_table(Spec, [Row | Rows], Length, IoList) ->
    create_table(Spec, Rows, Length, [row(Spec, Row) | IoList]).

-spec get_row_length(list(tuple())) -> non_neg_integer().
get_row_length(Spec) ->
    lists:foldl(fun({_Name, Size}, Total) ->
                    Total + Size + 2 
                end, 0, Spec) + 2.

-spec row(list(), list(string())) -> iolist().
row(Spec, Row) ->
    ["| " | lists:reverse(
        ["\n" | lists:foldl(fun({{_, Size}, Str}, Acc) ->
                                [align(Str, Size) | Acc]
                            end, [], lists:zip(Spec, Row))])].

-spec titles(list()) -> iolist().
titles(Spec) ->
    [ "| " | lists:reverse(
        ["\n" | lists:foldl(fun({Title, Size}, TitleRow) ->
                               [align(atom_to_list(Title), Size) | TitleRow]
                            end, [], Spec)])].

-spec align(string(), non_neg_integer()) -> iolist().
align(undefined, Size) ->
    align("", Size);
align(Str, Size) when is_integer(Str) ->
    align(integer_to_list(Str), Size);
align(Str, Size) when is_binary(Str) ->
    align(binary_to_list(Str), Size);
align(Str, Size) when is_atom(Str) ->
    align(atom_to_list(Str), Size);
align(Str, Size) when is_list(Str), length(Str) > Size -> 
    Truncated = lists:sublist(Str, Size),
    Truncated ++ " |"; 
align(Str, Size) when is_list(Str), length(Str) =:= Size ->
    Str ++ " |";
align(Str, Size) when is_list(Str) ->
    StrLen = length(Str),
    LeftSpaces = (Size - StrLen) div 2,
    RightSpaces = Size - StrLen - LeftSpaces,
    spaces(LeftSpaces) ++ Str ++ spaces(RightSpaces) ++ " |";
align(Term, Size) ->
    Str = lists:flatten(io_lib:format("~p", [Term])),
    align(Str, Size).

-spec vertical_border(non_neg_integer()) -> string().
vertical_border(Length) ->
    lists:reverse(["\n" | char_seq(Length, $-)]).

-spec spaces(non_neg_integer()) -> string().
spaces(Length) ->
    char_seq(Length, $\s).

-spec char_seq(non_neg_integer(), char()) -> string().
char_seq(Length, Char) ->
    [Char || _ <- lists:seq(1, Length)].

