%% @author Angela Gong <anjoola@anjoola.com>

%% @doc Module that includes helper functions used for grouping when
%%      aggregating.
-module(ydb_group).

-export([compute_grouped_schema/2, get_group_indexes/2, get_group_key/2,
    split_tuples/2]).

%% @headerfile "ydb_plan_node.hrl"
-include("ydb_plan_node.hrl").

%% ----------------------------------------------------------------- %%

-spec compute_grouped_schema(
    Schema :: ydb_plan_node:ydb_schema()
  , Columns :: [atom()]
) -> NewSchema :: ydb_plan_node:ydb_schema().

%% @doc Computes part of the schema containing just the grouped columns.
compute_grouped_schema(Schema, Columns) ->
    GroupIndexes = ydb_predicate_utils:get_indexes(
        true
      , Columns
      , dict:from_list(Schema)
    )
    
  , ColRange = lists:seq(1, length(GroupIndexes))
  , NewSchema = lists:map(fun(I) ->
        Index = lists:nth(I, GroupIndexes)
      , ydb_predicate_utils:get_col(I, Index, Columns, Schema, true) end
      , ColRange
    )
  , NewSchema
.

%% ----------------------------------------------------------------- %%

-spec split_tuples(
    Tuples :: [ydb_plan_node:ydb_tuple()]
  , GroupIndexes :: [integer()]
) ->
    [{GroupKey :: tuple(), GroupTuples :: [ydb_plan_node:ydb_tuple()]}]
.

%% @doc Splits a list of tuples into groups, depending on the indices
%%      of the columns to group by. Result is a list of group keys
%%      (which distinguishes groups), and the list of tuples in each
%%      group. Keys are in lexicograhic order.
split_tuples(Tuples, GroupIndexes) ->
    Mapping = lists:foldl(
        fun(Tuple, Dict) ->
            % Get the group for this tuple.
            GroupKey = get_group_key(Tuple, GroupIndexes)
          , orddict:append(GroupKey, Tuple, Dict)
        end
      , orddict:new()
      , Tuples
    )
  , orddict:to_list(Mapping)
.

-spec get_group_indexes(
    Schema :: ydb_plan_node:ydb_schema()
  , Columns :: [atom()]
) ->
    [integer()]
.
  
%% @doc Gets the indexes of the grouped columns.
get_group_indexes(Schema, Columns) ->
    GroupIndexes = ydb_predicate_utils:get_indexes(
        true
      , Columns
      , dict:from_list(Schema)
    )
  , GroupIndexes
.
  
-spec get_group_key(
    Tuple :: ydb_plan_node:ydb_tuple()
  , GroupIndexes :: [integer()]
) -> GroupKey :: tuple().

%% @doc Gets the group key of a tuple, given the list of indexes of the
%%      columns to group by.
get_group_key(Tuple, GroupIndexes) ->
    ListGroupKey = lists:map(fun(Index) ->
        element(Index, Tuple#ydb_tuple.data) end
      , GroupIndexes
    )
  , GroupKey = list_to_tuple(ListGroupKey)
  , GroupKey
.

%% ----------------------------------------------------------------- %%