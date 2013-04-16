%% @author Angela Gong <anjoola@anjoola.com>

%% @doc This module tests the input_node_utils functions.
-module(ydb_input_node_utils_test).

%% @headerfile "ydb_plan_node.hrl"
-include("include/ydb_plan_node.hrl").
-include_lib("eunit/include/eunit.hrl").

make_tuples_test() ->
    ?assertMatch(
        [#ydb_tuple{data={1}}, #ydb_tuple{data={2}}, #ydb_tuple{data={3}}]
      , ydb_input_node_utils:make_tuples(
            '$auto_timestamp'
          , [{col, {1,int}}]
          , [{1}, {2}, {3}]
        )
    )
  , ?assertMatch(
        [#ydb_tuple{data={1,blah,okay}}]
      , ydb_input_node_utils:make_tuples(
            '$auto_timestamp'
          , [{col, {1,double}}]
          , [{1,blah,okay}]
        )
    )
.

make_tuple_test() ->
    ?assertMatch(
        #ydb_tuple{timestamp=23000, data={23,blah}}
      , ydb_input_node_utils:make_tuple(
            {milli_sec, timecol}
          , [{timecol, {1,int}}, {other, {2,int}}]
          , {23,blah}
        )
    )
  , ?assertMatch(
        #ydb_tuple{timestamp=13, data={13,yay,blah}}
      , ydb_input_node_utils:make_tuple(
            {micro_sec, timecol}
          , [{timecol, {1,int}}, {other, {2,int}}, {more, {3,type}}]
          , {13,yay,blah}
        )
    )
  , ?assertMatch(
        #ydb_tuple{data={fundata}}
      , ydb_input_node_utils:make_tuple(
            '$auto_timestamp'
          , [{schema, {1,varchar}}]
          , {fundata}
        )
    )
.

new_tuple_test() ->
    ?assertMatch(
        #ydb_tuple{timestamp=334, data={data}}
      , ydb_input_node_utils:new_tuple(334, {data})
    )
  , ?assertMatch(
        #ydb_tuple{timestamp=19483, data={1,3,4,3,blah}}
      , ydb_input_node_utils:new_tuple(19483, {1,3,4,3,blah})
    )
.

convert_time_test() ->
    ?assertEqual(ydb_input_node_utils:convert_time({micro_sec, 49384}), 49384)
  , ?assertEqual(ydb_input_node_utils:convert_time({milli_sec, 29}), 29000)
  , ?assertEqual(ydb_input_node_utils:convert_time({sec, 2}), 2000000)
  , ?assertEqual(ydb_input_node_utils:convert_time({min, 3}), 180000000)
  , ?assertEqual(ydb_input_node_utils:convert_time({hour, 1}), 3600000000)
  , ?assertEqual(
      ydb_input_node_utils:convert_time({maxhhhhh, 2})
    , {error, {badarg, maxhhhhh}}
  )
  , ?assertEqual(
      ydb_input_node_utils:convert_time({sec, -32})
    , {error, {badarg, -32}}
  )
.
