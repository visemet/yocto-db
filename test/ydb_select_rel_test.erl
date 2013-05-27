%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc This module tests the SELECT mudle for relations.
-module(ydb_select_rel_test).
-export([start_link_test_helper/2]).

-include_lib("eunit/include/eunit.hrl").
-include("ydb_plan_node.hrl").

start_link_test() ->
    ?assert(start_link_test_1())
  , ?assert(start_link_test_2())
  , ?assert(start_link_test_3())
  , ?assert(start_link_test_4())
  , ?assert(start_link_test_5())
  , ?assert(start_link_test_6())
.

start_link_test_1() ->
    Predicate = {ydb_cv, num, 'gt', 12}
  , test_setup(Predicate, 5)
.

start_link_test_2() ->
    Predicate = {ydb_cv, num, 'eq', 12}
  , test_setup(Predicate, 3)
.

start_link_test_3() ->
    Predicate = {ydb_cv, num, 'lt', 5}
  , test_setup(Predicate, 1)
.

start_link_test_4() ->
    Predicate = {ydb_cv, num, 'ne', 12}
  , test_setup(Predicate, 11)
.

start_link_test_5() ->
    Predicate = {ydb_cv, num, 'gte', 12}
  , test_setup(Predicate, 8)
.

start_link_test_6() ->
    Predicate = {ydb_cv, num, 'lte', 5}
  , test_setup(Predicate, 2)
.

test_setup(Predicate, N) ->
    {DiffTid} = get_diff_tables()
  , DummyPid = test_setup_helper(Predicate, N)
  , ydb_plan_node:send_diffs(DummyPid, [DiffTid])
  , receive
        test_passed -> true
      ; fail -> false
    end
.

test_setup_helper(Predicate, N) ->
    Schema = [{num, {1, int}}]

    % Set up a dummy node to pass the schema over.
  , {ok, DummyPid} = ydb_file_input:start_link([
        {filename, "../test/data/select_test_helper.dta"}
      , {batch_size, 100}
      , {poke_freq, 1}
    ], [{schema, Schema}])

    % The select node
  , {ok, SelectPid} = ydb_select:start_link([
        {predicate, Predicate}
    ], [{listen, [DummyPid]}])

  , Listener = spawn(
        ?MODULE
      , start_link_test_helper
      , [self(), N]
    )
  , ydb_plan_node:add_listener(SelectPid, Listener)
  , DummyPid %AggrPid
.


start_link_test_helper(Pid, NumResults) ->
    receive
        {'$gen_cast', {relegate, {diffs, [DiffTid]}}} ->
            ?assertEqual(NumResults, ets:info(DiffTid, size))
          , Pid ! test_passed
      ; _Other -> Pid ! fail
    end
.

get_diff_tables() ->
    Diff1 = ets:new(diff1, [bag])
  , ets:insert(Diff1, {'+', {row, 1}, {ydb_tuple, 1, {3}}})
  , ets:insert(Diff1, {'+', {row, 2}, {ydb_tuple, 2, {9}}})
  , ets:insert(Diff1, {'+', {row, 3}, {ydb_tuple, 3, {5}}})
  , ets:insert(Diff1, {'+', {row, 4}, {ydb_tuple, 4, {7}}})
  , ets:insert(Diff1, {'+', {row, 4}, {ydb_tuple, 5, {8}}})
  , ets:insert(Diff1, {'+', {row, 4}, {ydb_tuple, 6, {12}}})
  , ets:insert(Diff1, {'+', {row, 4}, {ydb_tuple, 7, {6}}})
  , ets:insert(Diff1, {'+', {row, 4}, {ydb_tuple, 8, {73}}})
  , ets:insert(Diff1, {'+', {row, 4}, {ydb_tuple, 9, {12}}})
  , ets:insert(Diff1, {'+', {row, 4}, {ydb_tuple, 10, {13}}})
  , ets:insert(Diff1, {'+', {row, 4}, {ydb_tuple, 11, {15}}})
  , ets:insert(Diff1, {'+', {row, 4}, {ydb_tuple, 12, {21}}})
  , ets:insert(Diff1, {'+', {row, 4}, {ydb_tuple, 13, {12}}})
  , ets:insert(Diff1, {'+', {row, 4}, {ydb_tuple, 14, {23}}})
  , {Diff1}
.