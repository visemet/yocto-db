%% @author Angela Gong <anjoola@anjoola.com>

%% @doc This module contains utility functions used for aggregate
%%      functions.
-module(ydb_aggr_utils).

%% @headerfile "ydb_plan_node.hrl"
-include("ydb_plan_node.hrl").

-export([compute_new_schema/3, get_col/2]).

% Testing for private functions.
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% ----------------------------------------------------------------- %%

-spec get_col(
    ColName :: atom()
  , Schema :: dict())
->
    {Index :: integer(), Type :: atom()} | error.

%% @doc Gets the index and type of a particular column.
get_col(ColName, Schema) ->
    case dict:find(ColName, Schema) of
        {ok, {Index, Type}} ->
            {Index, Type}
      ; error -> error
    end
.

%% ----------------------------------------------------------------- %%

-spec compute_new_schema(
    Schema :: ydb_plan_node:ydb_schema()
  , Column :: atom() | {atom(), atom()}
  , Aggregate :: string()
) ->
    {Index :: integer(), NewSchema :: ydb_plan_node:ydb_schema()}
.

%% @doc Computes the new schema for a particular aggregate.
compute_new_schema(Schema, {OldCol, NewCol}, Aggregate) ->
    {Index, Type} = get_col(OldCol, dict:from_list(Schema))
  , NewColName =
        list_to_atom(Aggregate ++ "(" ++ atom_to_list(NewCol) ++ ")")
  , NewSchema = [{NewColName, {1, Type}}]
  , {Index, NewSchema}
;

compute_new_schema(Schema, Column, Aggregate) ->
    compute_new_schema(Schema, {Column, Column}, Aggregate)
.

%% ----------------------------------------------------------------- %%
