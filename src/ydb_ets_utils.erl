%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc This module serves as an interface for plan_nodes to interact
%%      with the ets tables.
-module(ydb_ets_utils).

%% @headerfile "ydb_plan_node.hrl"
-include("ydb_plan_node.hrl").

-export([get_copy/2, combine_partial_results/2, add_tuples/3,
    delete_table/1, create_table/1, dump_raw/1, dump_tuples/1,
    apply_diffs/2]).


%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-spec get_copy(
    Tid :: ets:tid()
  , NewName :: atom()
) ->
    {ok, Tid :: ets:tid()}
.

%% @doc Returns a complete copy of the table.
get_copy(Tid, Name) ->
    NewTid = ets:new(Name, [])
  , Tuples = ets:match_object(Tid, '_')
  , ets:insert(NewTid, Tuples)
  , {ok, NewTid}
.

%% ----------------------------------------------------------------- %%

-spec dump_raw(Tid :: ets:tid()) -> [tuple()].

%% @doc Returns a list of all the rows in the table as tuples.
%%      Tuples will typically be in the format {key, ydb_tuple}, where
%%      key is either {'row_no', Count} or {Op, Timestamp}.
dump_raw(Tid) ->
    ets:match_object(Tid, '_').

%% ----------------------------------------------------------------- %%

-spec dump_tuples(Tid :: ets:tid()) -> [ydb_tuple()].

%% @doc Returns a list of all the ydb_tuples in the table.
dump_tuples(Tid) ->
    lists:flatten(ets:match(Tid, {'_', '$1'})).

%% ----------------------------------------------------------------- %%

-spec create_table(
    Name :: atom()
) ->
    {ok, Tid:: ets:tid()}
.

%% @doc Creates a new ets table with the given name.
create_table(Name) ->
    Tid = ets:new(Name, []) % TODO add options
  , {ok, Tid}
.

%% ----------------------------------------------------------------- %%

-spec add_tuples(
    Tid :: ets:tid()
  , Op :: atom()
  , TupleOrTuples :: ydb_tuple() | [ydb_tuple()]
) ->
    {ok}
.

%% @doc Adds tuples to the specified table, using {Op, Timestamp}
%%      as the key. This table is assumed to be in synopsis format.
add_tuples(Tid, Op, Tuples) when is_list(Tuples) ->
    lists:foreach(fun(X) -> add_tuples(Tid, Op, X) end, Tuples)
  , {ok}
;
add_tuples(Tid, Op, Tuple=#ydb_tuple{timestamp=Timestamp}) ->
    ets:insert(Tid, {{Op, Timestamp}, Tuple})
  , {ok}
.

%% ----------------------------------------------------------------- %%

-spec delete_table(Tid :: ets:tid()) -> {ok}.

%% @doc Deletes the specified table.
delete_table(Tid) ->
    ets:delete(Tid)
  , {ok}
.

%% ----------------------------------------------------------------- %%

-spec combine_partial_results(
    Tids :: [ets:tid()]
  , NewName :: atom()
) ->
    {ok, Tid :: ets:tid()}
.

%% @doc Inserts the tuples from multiple ets tables into a single new
%%      table, using {'row_no', Count} as the key. The new table is
%%      in relation format.
combine_partial_results(Tids, NewName) ->
    {ok, NewTid} = create_table(NewName)
  , Tuples = lists:flatten(
        lists:map(fun(X) -> ets:match(X, {'_', '$1'}) end, Tids))
  , ets:insert(NewTid, create_relation_tuples(Tuples, 0))
  , {ok, NewTid}
.

%% ----------------------------------------------------------------- %%

-spec apply_diffs(
    BaseTid :: ets:tid()
  , DiffTids :: [ets:tid()]
) ->
    {ok}.

%% @doc Applies the diffs in the specified table to the base table.
apply_diffs(_BaseTid, _DiffTids) ->
    {ok} %TODO implement
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

%% @doc Takes in a list of tuples and adds a relation key to the front
%%      of each tuple in the list. Converts tuples as follows:
%%      ydb_tuple -> {{'row_no', Count}, ydb_tuple}, where Count is
%%      a monotomically increasing integer.
-spec create_relation_tuples(
    Tuples :: [tuple()]
  , Start :: integer()
) ->
    [tuple()].

create_relation_tuples(Tuples, Start) ->
    lists:zipwith(fun(X, Y) -> {{'row_num', X}, Y} end,
        lists:seq(Start, Start + length(Tuples) - 1),
        Tuples)
.

