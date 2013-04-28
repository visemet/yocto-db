%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc This module serves as an interface for plan_nodes to interact
%%      with the ets tables.
-module(ydb_ets_utils).

%% @headerfile "ydb_plan_node.hrl"
-include("ydb_plan_node.hrl").

-export([create_table/1, delete_table/1, add_tuples/3, delete_tuples/2,
    delete_tuples/3, replace_tuple/4]).
-export([dump_raw/1, dump_tuples/1, dump_tuples/2, extract_diffs/1,
    extract_timestamps/1, extract_timestamps/2, get_copy/2]).
-export([apply_diffs/2, add_diffs/4, combine_partial_results/2,
    max_timestamp/1, max_timestamp/2]).


%%% =============================================================== %%%
%%%  internal records and types                                     %%%
%%% =============================================================== %%%

-type diff() :: '+' | '-'.

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
    lists:flatten(ets:match(Tid, {'_', '$1'}))
.

-spec dump_tuples(Tid :: ets:tid(), Key :: atom()) -> [ydb_tuple()].

%% @doc Returns a list of all the ydb_tuples of a particular Op in
%%      the table.
dump_tuples(Tid, Op) ->
    lists:flatten(ets:match(Tid, {{Op, '_'}, '$1'}))
.

%% ----------------------------------------------------------------- %%

-spec create_table(
    Name :: atom()
) ->
    {ok, Tid:: ets:tid()}
.

%% @doc Creates a new ets table with the given name.
create_table(Name) ->
    Tid = ets:new(Name, [bag]) % TODO add options
  , {ok, Tid}
.

%% ----------------------------------------------------------------- %%

-spec add_tuples(
    Tid :: ets:tid()
  , Op :: atom()
  , TupleOrTuples :: ydb_plan_node:ydb_tuple() | [ydb_plan_node:ydb_tuple()]
) ->
    ok
.

%% @doc Adds tuples to the specified table, using {Op, Timestamp}
%%      as the key. This table is assumed to be in synopsis format.
add_tuples(Tid, Op, Tuples) when is_list(Tuples) ->
    lists:foreach(fun(X) -> add_tuples(Tid, Op, X) end, Tuples)
  , ok
;
add_tuples(Tid, Op, Tuple=#ydb_tuple{timestamp=Timestamp}) ->
    ets:insert(Tid, {{Op, Timestamp}, Tuple})
  , ok
.
%% ----------------------------------------------------------------- %%

-spec delete_tuples(
    Tid :: ets:tid()
  , {Op :: atom(), Timestamp :: non_neg_integer()}
) -> ok.

%% @doc Removes specific tuples with a specified key from the table.
%%      This is assumed to be in the synopsis format.
delete_tuples(Tid, {Op, Timestamp}) ->
    ets:delete(Tid, {Op, Timestamp})
  , ok
.

%% ----------------------------------------------------------------- %%

-spec delete_tuples(
    Tid :: ets:tid()
  , Op :: atom()
  , TupleOrTuples :: ydb_tuple() | [ydb_tuple()]
) ->
    {ok}
.

%% @doc Deletes tuples from the specified table, using {Op, Timestamp}
%%      as the key. This table is assumed to be in synopsis format.
delete_tuples(Tid, Op, Tuples) when is_list(Tuples) ->
    lists:foreach(fun(X) -> delete_tuples(Tid, Op, X) end, Tuples)
  , {ok}
;
delete_tuples(Tid, Op, Tuple=#ydb_tuple{timestamp=Timestamp}) ->
    ets:delete_object(Tid, {{Op, Timestamp}, Tuple})
  , {ok}
.


%% ----------------------------------------------------------------- %%

-spec delete_table(Tid :: ets:tid()) -> ok.

%% @doc Deletes the specified table.
delete_table(Tid) ->
    ets:delete(Tid)
  , ok
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
  , DiffTidOrTids :: ets:tid() | [ets:tid()]
) ->
    ok
.

%% @doc Applies the diffs in the specified table to the base table.
apply_diffs(BaseTid, DiffTids) when is_list(DiffTids) ->
    % The MatchSpecs below are produced with the following commands:
    %   InsSpec = ets:fun2ms(fun ({{'+', Op, Timestamp}, Tuple}) ->
    %                   {{Op, Timestamp}, Tuple} end).
    %   DelSpec = ets:fun2ms(fun ({{'-', Op, Timestamp}, Tuple}) ->
    %                   {{Op, Timestamp}, Tuple} end).

    InsSpec = [{{{'+','$1','$2'},'$3'},[],[{{{{'$1','$2'}},'$3'}}]}]
  , Inserts = lists:flatten(
        lists:map(fun(X) -> ets:select(X, InsSpec) end, DiffTids))
  , ets:insert(BaseTid, Inserts)
  , DelSpec = [{{{'-','$1','$2'},'$3'},[],[{{{{'$1','$2'}},'$3'}}]}]
  , Deletes = lists:flatten(
        lists:map(fun(X) -> ets:select(X, DelSpec) end, DiffTids))
  , lists:foreach(fun(X) -> ets:delete_object(BaseTid, X) end, Deletes)
  , ok
;

apply_diffs(BaseTid, DiffTid) ->
    apply_diffs(BaseTid, [DiffTid])
.

%% ----------------------------------------------------------------- %%

-spec add_diffs(
    Tid :: ets:tid()
  , Diff :: diff()
  , Op :: atom()
  , TupleOrTuples :: ydb_plan_node:ydb_tuple() | [ydb_plan_node:ydb_tuple()]
) ->
    ok
.

%% @doc Adds a diff to the specified table, using {Diff, Op, Timestamp}
%%      as the key.
add_diffs(
    Tid
  , Diff
  , Op
  , Tuple=#ydb_tuple{timestamp=Timestamp}
) when is_tuple(Tuple) ->
    ets:insert(Tid, {{Diff, Op, Timestamp}, Tuple})
  , ok
;

add_diffs(Tid, Diff, Op, Tuples) when is_list(Tuples) ->
    lists:foreach(fun(X) -> add_diffs(Tid, Diff, Op, X) end, Tuples)
  , ok
.

%% ----------------------------------------------------------------- %%

-spec extract_diffs(
    DiffTids :: [ets:tid()]
) ->
    {Ins :: [ydb_plan_node:ydb_tuple()], Dels :: [ydb_plan_node:ydb_tuple()]}
.

%% @doc Returns the tuples in the table separated into a list of
%%      tuples to be added, and to be deleted.
extract_diffs(DiffTids) ->
    % The MatchSpecs below are produced with the following commands:
    %   InsSpec = ets:fun2ms(fun ({{'+', _, _}, Tuple}) -> Tuple end).
    %   DelSpec = ets:fun2ms(fun ({{'-', _, _}, Tuple}) -> Tuple end).

    InsSpec = [{{{'+','_','_'},'$1'}, [], ['$1']}]
  , Inserts = lists:flatten(
        lists:map(fun(X) -> ets:select(X, InsSpec) end, DiffTids))

  , DelSpec = [{{{'-','_','_'},'$1'}, [], ['$1']}]
  , Deletes = lists:flatten(
        lists:map(fun(X) -> ets:select(X, DelSpec) end, DiffTids))
  , {Inserts, Deletes}
.

%% ----------------------------------------------------------------- %%

-spec extract_timestamps(
    Tuples :: [ydb_plan_node:ydb_tuple()]
) ->
    Timestamps :: [non_neg_integer()]
.

%% @doc Extracts the timestamps from a list of tuples.
extract_timestamps(Tuples) ->
    Timestamps = lists:map(fun(Tuple) ->
        Tuple#ydb_tuple.timestamp end
      , Tuples
    )
  , Timestamps
.

-spec extract_timestamps(
    Tids :: [ets:tid()]
  , TableType :: atom()
) ->
    Timestamps :: [non_neg_integer()]
.

%% ----------------------------------------------------------------- %%

%% @doc Extracts the timestamps from a list of ETS tables.
extract_timestamps(Tids, syn) ->
    extract_timestamps(Tids, rel)
;
extract_timestamps(Tids, rel) ->
    Spec = [{{{'_','$1'},'_'},[],['$1']}]
  , lists:flatten(
        lists:map(fun(X) -> ets:select(X, Spec) end, Tids)
    )
;
extract_timestamps(Tids, diff) ->
    Spec = [{{{'_','_','$1'},'_'},[],['$1']}]
  , lists:flatten(
        lists:map(fun(X) -> ets:select(X, Spec) end, Tids)
    )
.

%% ----------------------------------------------------------------- %%

-spec max_timestamp(
    Tuples :: [ydb_plan_node:ydb_tuple()]
) -> Timestamp :: non_neg_integer().

%% @doc Finds the maximum timestamp from a list of tuples.
max_timestamp(Tuples) when is_list(Tuples) ->
    lists:foldl(
        fun(X, Max) -> max(X, Max) end
      , -1
      , extract_timestamps(Tuples)
    )
.

-spec max_timestamp(
    Tids :: [ets:tid()]
  , TableType :: atom()
) ->
    Timestamps :: non_neg_integer()
.

%% @doc Finds the maximum timestamp stored in a list of ETS tables.
max_timestamp(Tids, TableType) ->
    lists:foldl(
        fun(X, Max) -> max(X, Max) end
      , -1
      , extract_timestamps(Tids, TableType)
    )
.

%% ----------------------------------------------------------------- %%

-spec replace_tuple(
    Tid :: ets:tid()
  , Type :: atom()
  , OldTuple :: ydb_tuple()
  , NewTuple :: ydb_tuple()
) ->
    ok
.

%% @doc Replaces a tuple in the given table.
replace_tuple(Tid, Type, OldTuple=#ydb_tuple{}, NewTuple=#ydb_tuple{}) ->
    ydb_ets_utils:delete_tuples(Tid, Type, OldTuple)
  , ydb_ets_utils:add_tuples(Tid, Type, NewTuple)
  , ok
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec create_relation_tuples(
    Tuples :: [ydb_tuple()]
  , Start :: integer()
) ->
    [tuple()]
.

%% @doc Takes in a list of tuples and adds a relation key to the front
%%      of each tuple in the list. Converts tuples as follows:
%%      ydb_tuple -> {{'row_num', Count}, ydb_tuple}, where Count is
%%      a monotomically increasing integer.
create_relation_tuples(Tuples, Start) ->
    lists:zipwith(fun(X, Y) -> {{'row_num', X}, Y} end,
        lists:seq(Start, Start + length(Tuples) - 1),
        Tuples)
.