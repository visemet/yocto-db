%% @author Angela Gong <anjoola@anjoola.com>

%% @private
%% @doc This module contains utility functions used for predicate
%%      comparisons.
-module(ydb_predicate_utils).

-export([get_col/3, get_col/5, get_indexes/3, is_satisfied/3]).

%% @headerfile "ydb_plan_node.hrl"
-include("ydb_plan_node.hrl").

% Testing for private functions.
-ifdef(TEST).
-export([check_satisfied/3, compare_types/3, compare/3]).
-export([get_value/3, is_same_type/2]).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% ----------------------------------------------------------------- %%

-spec is_satisfied(
    Tuple :: ydb_plan_node:ydb_tuple()
  , Schema :: ydb_plan_node:ydb_schema()
  , Predicate :: ydb_plan_node:ydb_clause()
) -> boolean().

%% @doc Converts the schema to a dictionary for easier use. Passes it
%%      along to a helper function to evaluation satisfaction.
is_satisfied(Tuple, Schema, Predicate) ->
    check_satisfied(Tuple, dict:from_list(Schema), Predicate)
.

-spec check_satisfied(
    Tuple :: ydb_plan_node:ydb_tuple()
  , Schema :: dict()
  , Predicate :: ydb_plan_node:ydb_clause()
) -> boolean().

%% @doc Checks to see if the tuple of a particular schema is satisfied
%%      by the predicate.
check_satisfied(Tuple, Schema, #ydb_and{clauses=Clauses}) ->
    BooleanClauses = lists:map(fun(Clause) ->
        check_satisfied(Tuple, Schema, Clause) end, Clauses
    )
  , Satisfied = lists:foldl(fun(Boolean, Result) ->
        Boolean and Result end, true, BooleanClauses
    )
  , Satisfied
;

check_satisfied(Tuple, Schema, #ydb_or{clauses=Clauses}) ->
    BooleanClauses = lists:map(fun(Clause) ->
        check_satisfied(Tuple, Schema, Clause) end, Clauses
    )
  , Satisfied = lists:foldl(fun(Boolean, Result) ->
        Boolean or Result end, false, BooleanClauses
    )
  , Satisfied
;

check_satisfied(Tuple, Schema, #ydb_not{clause=Clause}) ->
    Satisfied = check_satisfied(Tuple, Schema, Clause)
  , not Satisfied
;

check_satisfied(
    Tuple
  , Schema
  , _Query = #ydb_cv{column=Col, operator=Op, value=Value}
) ->
    ActualValue = get_value(Tuple, Schema, Col)
  , compare_types(ActualValue, Op, Value)
;

check_satisfied(
    Tuple
  , Schema
  , _Query = #ydb_cc{left_col=Left, operator=Op, right_col=Right}
) ->
    {LValue, RValue} = {
        get_value(Tuple, Schema, Left)
      , get_value(Tuple, Schema, Right)
    }
  , compare_types(LValue, Op, RValue)
;

check_satisfied(_Tuple, _Schema, _Predicate) -> false.

%% ----------------------------------------------------------------- %%

-spec compare_types(
    LValue :: term()
  , Pperator :: ydb_plan_node:compare()
  , RValue :: term()
) -> boolean().

%% @doc Makes sure that the values being compared are of similar types.
compare_types(LValue, Operator, RValue) ->
    SameType = is_same_type(LValue, RValue)
  , if
        SameType ->
            compare(LValue, Operator, RValue)
      ; true ->
            erlang:error(incompatible_type_comparisons)
    end
.

-spec is_same_type(
    LValue :: term()
  , RValue :: term()
) -> boolean().

%% @doc Compares the types of two values and sees if they are equal.
is_same_type(LValue, RValue)
  when
      (is_integer(LValue) orelse is_float(LValue))
    , (is_integer(RValue) orelse is_float(RValue))
->
    true
;

is_same_type(LValue, RValue) ->
    SameType = (is_atom(LValue) orelse io_lib:printable_list(LValue)) and
               (is_atom(RValue) orelse io_lib:printable_list(RValue))
  , if SameType -> true
        ; true -> false
    end
.

-spec compare(
    LValue :: term()
  , Operator :: ydb_plan_node:compare()
  , RValue :: term()
) -> boolean().

%% @doc Compares two values with an operator and returns whether or not
%%      it is satisfied.
compare(LValue, 'gt', RValue) ->
    LValue > RValue
;

compare(LValue, 'lt', RValue) ->
    LValue < RValue
;

compare(LValue, 'eq', RValue) ->
    LValue == RValue
;
    
compare(LValue, 'gte', RValue) ->
    LValue >= RValue
;
    
compare(LValue, 'lte', RValue) ->
    LValue =< RValue
;
    
compare(LValue, 'ne', RValue) ->
    LValue /= RValue
;

compare(_LValue, _Op, _RValue) ->
    false
.

%% ----------------------------------------------------------------- %%

-spec get_value(
    Tuple :: ydb_plan_node:ydb_tuple()
  , Schema :: dict()
  , Col :: atom()
) -> Value :: term() | error.

%% @doc Gets the value of the tuple at a particular column.
get_value(_Tuple = #ydb_tuple{data=Data}, Schema, Col) ->
    case dict:find(Col, Schema) of
        {ok, {Index, _Type}} ->
            Val = element(Index, Data)
          , Val
      % If the column cannot be found.
      ; error -> erlang:error(unknown_column)
    end
.

%% ----------------------------------------------------------------- %%

-spec get_indexes(
    Include :: boolean()
  , Columns :: [atom() | {atom(), atom()}]
  , Schema :: dict())
-> Indexes :: [integer()].

%% @doc Gets the list of indexes of the desired columns, based on a
%%      list of column names that are supposed to be excluded or
%%      included.
get_indexes(_Include=true, Columns, Schema) ->
    Indexes = lists:map(fun(Col) ->
        get_index(Col, Schema) end, Columns
    )
  , Indexes
;

get_indexes(_Include=false, Columns, Schema) ->
    BadIndexes = get_indexes(true, Columns, Schema)
  , Indexes = lists:seq(1, length(dict:to_list(Schema)))
  , lists:subtract(Indexes, BadIndexes)
.

-spec get_index(
    ColName :: atom() | {atom(), atom()}
  , Schema :: dict())
-> integer() | error.

%% @doc Gets the index of a particular column.
get_index(_Col={OldName, _NewName}, Schema) ->
    get_index(OldName, Schema)
;

get_index(ColName, Schema) ->
    case dict:find(ColName, Schema) of
        {ok, {Index, _Type}} ->
            Index
      ; error -> error
    end
.

%% ----------------------------------------------------------------- %%

-spec get_col(
    I :: integer()
  , Index :: integer()
  , Schema :: ydb_plan_node:ydb_schema()
) -> {ColName :: atom(), {I :: integer(), Type :: atom()}}.

%% @doc Re-numbers a column.
get_col(I, Index, Schema) ->
    {Name, {_Index, Type}} = lists:nth(Index, Schema)
  , {Name, {I, Type}}
.

-spec get_col(
    I :: integer()
  , Index :: integer()
  , Columns :: [atom() | {atom(), atom()}]
  , Schema :: ydb_plan_node:ydb_schema()
  , Include :: boolean()
) -> {ColName :: atom(), {I :: integer(), Type :: atom()}}.

%% @doc Returns the column at a particular index in the schema.
%%      Renames the column if desired. Column will now have a new
%%      column number of I.
get_col(I, Index, Columns, Schema, _Include=true) ->
    % Columns is the list of columns we do want, and their renames.
    Column = lists:nth(I, Columns)
  , {_OldName, {_Index, Type}} = lists:nth(Index, Schema)
  , case Column of
        {_OldName, NewName} -> {NewName, {I, Type}}
      ; ColName -> {ColName, {I, Type}}
    end
;

get_col(I, Index, _Columns, Schema, _Include=false) ->
    % Index is the index of the column we do want from Schema.
    {ColName, {_Index, Type}} = lists:nth(Index, Schema)
  , {ColName, {I, Type}}
.

%% ----------------------------------------------------------------- %%
