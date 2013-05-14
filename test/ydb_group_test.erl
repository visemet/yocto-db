%% @author Angela Gong <anjoola@anjoola.com>

%% @doc This module tests the grouping function.
-module(ydb_group_test).

-include_lib("eunit/include/eunit.hrl").

compute_grouped_schema_test() ->
    Schema = [{a, {1, int}}, {b, {2, int}}, {c, {3, int}}, {d, {4, int}}]

  , ?assertMatch(
        [{b, {1, int}}, {d, {2, int}}]
      , ydb_group:compute_grouped_schema(Schema, [b, d])
    )
  , ?assertMatch(
        [{c, {1, int}}, {a, {2, int}}, {b, {3, int}}, {d, {4, int}}]
      , ydb_group:compute_grouped_schema(Schema, [c, a, b, d])
    )
  , ?assertMatch(
        [{d, {1, int}}]
      , ydb_group:compute_grouped_schema(Schema, [d])
    )
.

split_tuples_test() ->
    Tuples = [
        {ydb_tuple, 1, {angela, 523, ca, cs, female}}
      , {ydb_tuple, 1, {max, 2345, mi, cs, male}}
      , {ydb_tuple, 1, {kalpana, 654, pa, cs, female}}
      , {ydb_tuple, 1, {audrey, 6652, ca, bio, female}}
      , {ydb_tuple, 1, {mike, 754, fl, cs, male}}
      , {ydb_tuple, 1, {curie, 763, pa, bio, female}}
      , {ydb_tuple, 1, {david, 75, ca, cs, male}}
      , {ydb_tuple, 1, {jesse, 156, fl, cs, male}}
      , {ydb_tuple, 1, {howard, 70, ca, bio, male}}
    ]
  , ?assertMatch(
        [
            {{female}, [
                {ydb_tuple, 1, {angela, 523, ca, cs, female}}
              , {ydb_tuple, 1, {kalpana, 654, pa, cs, female}}
              , {ydb_tuple, 1, {audrey, 6652, ca, bio, female}}
              , {ydb_tuple, 1, {curie, 763, pa, bio, female}}
            ]}
          , {{male}, [
                {ydb_tuple, 1, {max, 2345, mi, cs, male}}
              , {ydb_tuple, 1, {mike, 754, fl, cs, male}}
              , {ydb_tuple, 1, {david, 75, ca, cs, male}}
              , {ydb_tuple, 1, {jesse, 156, fl, cs, male}}
              , {ydb_tuple, 1, {howard, 70, ca, bio, male}}
            ]}
        ]
      , ydb_group:split_tuples(Tuples, [5])
    )
  , ?assertMatch(
        [
            {{bio, female}, [
                {ydb_tuple, 1, {audrey, 6652, ca, bio, female}}
              , {ydb_tuple, 1, {curie, 763, pa, bio, female}}
            ]}
          , {{bio, male}, [
                {ydb_tuple, 1, {howard, 70, ca, bio, male}}
            ]}
          , {{cs, female}, [
                {ydb_tuple, 1, {angela, 523, ca, cs, female}}
              , {ydb_tuple, 1, {kalpana, 654, pa, cs, female}}
            ]}
          , {{cs, male}, [
                {ydb_tuple, 1, {max, 2345, mi, cs, male}}
              , {ydb_tuple, 1, {mike, 754, fl, cs, male}}
              , {ydb_tuple, 1, {david, 75, ca, cs, male}}
              , {ydb_tuple, 1, {jesse, 156, fl, cs, male}}
            ]}
        ]
      , ydb_group:split_tuples(Tuples, [4, 5])
    )
  , ?assertMatch(
        [
            {{ca}, [
                {ydb_tuple, 1, {angela, 523, ca, cs, female}}
              , {ydb_tuple, 1, {audrey, 6652, ca, bio, female}}
              , {ydb_tuple, 1, {david, 75, ca, cs, male}}
              , {ydb_tuple, 1, {howard, 70, ca, bio, male}}
            ]}
          , {{fl}, [
                {ydb_tuple, 1, {mike, 754, fl, cs, male}}
              , {ydb_tuple, 1, {jesse, 156, fl, cs, male}}
            ]}
          , {{mi}, [
                {ydb_tuple, 1, {max, 2345, mi, cs, male}}
            ]}
          , {{pa}, [
                {ydb_tuple, 1, {kalpana, 654, pa, cs, female}}
              , {ydb_tuple, 1, {curie, 763, pa, bio, female}}
            ]}
        ]
      , ydb_group:split_tuples(Tuples, [3])
    )
.

get_group_indexes_test() ->
    Schema = [{a, {1, int}}, {b, {2, int}}, {c, {3, int}}, {d, {4, int}}]

  , ?assertMatch([2, 4], ydb_group:get_group_indexes(Schema, [b, d]))
  , ?assertMatch(
        [3, 1, 2, 4]
      , ydb_group:get_group_indexes(Schema, [c, a, b, d])
    )
  , ?assertMatch([4], ydb_group:get_group_indexes(Schema, [d]))
.

get_group_key_test() ->
    Schema = [{a, {1, int}}, {b, {2, int}}, {c, {3, int}}, {d, {4, int}}]
    
  , ?assertMatch({dinner, lunch}, ydb_group:get_group_key(
        {ydb_tuple, 1, {breakfast, lunch, snack, dinner}}
      , ydb_group:get_group_indexes(Schema, [d, b])
    ))
  , ?assertMatch({4, 3, 2, 1}, ydb_group:get_group_key(
        {ydb_tuple, 1, {3, 1, 4, 2}}
      , ydb_group:get_group_indexes(Schema, [c, a, d, b])
    ))
  , ?assertMatch({anjoola, cool}, ydb_group:get_group_key(
        {ydb_tuple, 1, {anjoola, is, very, cool}}
      , ydb_group:get_group_indexes(Schema, [a, d])
    ))
.
