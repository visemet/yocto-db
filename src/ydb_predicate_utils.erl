%% @author Angela Gong <anjoola@anjoola.com>

%% @private
%% @doc This module contains utility functions used for predicate
%%      comparisons.
-module(ydb_predicate_utils).

% TODO remove
-export([is_satisfied/3]).
-export([compare/3,get_value/3]).

-include_lib("ydb_plan_node.hrl").

%% ----------------------------------------------------------------- %%

-spec is_satisfied(
    Tuple :: ydb_tuple()
  , Schema :: ydb_schema()
  , Predicate :: clause()
) -> boolean().

%% @doc Converts the schema to a dictionary for easier use. Passes it
%%      along to a helper function to evaluation satisfaction.
is_satisfied(Tuple, Schema, Predicate) ->
    check_satisfied(Tuple, dict:from_list(Schema), Predicate)
.

-spec check_satisfied(
    Tuple :: ydb_tuple()
  , Schema :: dict()
  , Predicate :: clause()
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
  , compare(ActualValue, Op, Value)
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
  , compare(LValue, Op, RValue)
;

check_satisfied(_Tuple, _Schema, _Predicate) -> false.

%% ----------------------------------------------------------------- %%

-spec compare(
    LValue :: term()
  , Operator :: compare()
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
    Tuple :: #ydb_tuple{}
  , Schema :: dict()
  , Col :: atom()
) ->
    Value :: term() | error
.

%% @doc Gets the value of the tuple at a particular column.
get_value(_Tuple = #ydb_tuple{data=Data}, Schema, Col) ->
    case dict:find(Col, Schema) of
        {ok, {Index, _Type}} ->
            Val = element(Index, Data)
          , Val
      ; error -> error
    end
.

% TODO handle errors
% TODO make sure only valid types are compared. i.e. not comparing strings
% to an int?
% TODO be able to compare timestamps


%% ----------------------------------------------------------------- %%
