%% @doc The following types describe an abstract format for status information.
%% Each type has a semantic, organizational meaning in a way similar to an html
%% document. The difference here is that we want our format to use erlang
%% data structures and types and be able to generate human readable output, json,
%% csv and a subset of html, as well as other possible output.
-type value() :: {value, term()}.
-type text() :: {text, iolist()}.
-type schema() :: [any()].
%% A row where the type of each element matches it's place in the schema. A row
%% is unlabeled data and is only contained inside a table.
-type row() :: [any()].
-type rows() :: [row()].
%% A list of the same type of data. It's really just a labelled list. 
-type column() :: {column, iolist(), [iolist()]}.
-type table() :: {table, schema(), rows()} | {table, [column()]}.
-type alert() :: {alert, [column() | table() | text()]}.
-type ratio() :: {ratio, integer(), pos_integer() | neg_integer()}.
-type elem() :: text() | schema() | row() | column() | table() | alert() | ratio() | value.
-type status() :: [elem()].
