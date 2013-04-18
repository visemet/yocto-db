%% @author Angela Gong <anjoola@anjoola.com>

%% @doc This module tests the predicate_utils functions.
-module(ydb_predicate_utils_test).

%% @headerfile "ydb_plan_node.hrl"
-include("ydb_plan_node.hrl").
-include_lib("eunit/include/eunit.hrl").

is_same_type_test() ->
    ?assert(ydb_predicate_utils:is_same_type(34, 3))
  , ?assert(ydb_predicate_utils:is_same_type(3.0, 5))
  , ?assert(ydb_predicate_utils:is_same_type(5, 2.4))
  , ?assertNot(ydb_predicate_utils:is_same_type("234", 234))
  , ?assertNot(ydb_predicate_utils:is_same_type(355.3, "355.3"))
  , ?assert(ydb_predicate_utils:is_same_type(hi, "hi"))
  , ?assert(ydb_predicate_utils:is_same_type("okay", atomhere))
.

compare_types_test() ->
    ?assertError(
        incompatible_type_comparisons
      , ydb_predicate_utils:compare_types(2, 'gt', "34")
    )
  , ?assertError(
        incompatible_type_comparisons
      , ydb_predicate_utils:compare_types(hi, 'lt', 453)
    )
  , ?assert(ydb_predicate_utils:compare_types(2, 'gt', 1))
  , ?assert(ydb_predicate_utils:compare_types("asdf", 'lt', "asmt"))
  , ?assert(ydb_predicate_utils:compare_types(hi, 'eq', hi))
  , ?assert(ydb_predicate_utils:compare_types(42, 'ne', 43))
  , ?assert(ydb_predicate_utils:compare_types("A", 'lte', "a"))
  , ?assert(ydb_predicate_utils:compare_types("b", 'lt', "ba"))
  , ?assertNot(ydb_predicate_utils:compare_types(26, 'not', 26))
.
