%% @author Angela Gong <anjoola@anjoola.com>

%% @doc This module tests the time_utils functions.
-module(ydb_time_utils_test).

%% @headerfile "ydb_plan_node.hrl"
-include("include/ydb_plan_node.hrl").
-include_lib("eunit/include/eunit.hrl").

convert_time_test() ->
    ?assertEqual(ydb_time_utils:convert_time({micro_sec, 49384}), 49384)
  , ?assertEqual(ydb_time_utils:convert_time({milli_sec, 29}), 29000)
  , ?assertEqual(ydb_time_utils:convert_time({sec, 2}), 2000000)
  , ?assertEqual(ydb_time_utils:convert_time({min, 3}), 180000000)
  , ?assertEqual(ydb_time_utils:convert_time({hour, 1}), 3600000000)
  , ?assertEqual(
      ydb_time_utils:convert_time({maxhhhhh, 2})
    , {error, {badarg, {maxhhhhh, 2}}}
  )
  , ?assertEqual(
      ydb_time_utils:convert_time({sec, -32})
    , {error, {badarg, {sec, -32}}}
  )
.
