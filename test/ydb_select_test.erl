%% @author Angela Gong <anjoola@anjoola.com>

%% @doc This module tests the select node functions.
-module(ydb_select_test).
-export([start_link_test_helper/3]).

-include_lib("eunit/include/eunit.hrl").

start_link_test() ->
    ?assert(start_link_test_1())
  , ?assert(start_link_test_2())
  , ?assert(start_link_test_3())
  , ?assert(start_link_test_4())
  , ?assert(start_link_test_5())
  , ?assert(start_link_test_6())
.

start_link_test_1() ->
    Predicate = {ydb_cv, num, 'gt', 50}
  , test_setup(Predicate, 37)
.

start_link_test_2() ->
    Predicate = {ydb_cv, num, 'eq', 97}
  , test_setup(Predicate, 2)
.

start_link_test_3() ->
    Predicate = {ydb_cv, num, 'lt', 47}
  , test_setup(Predicate, 12)
.

start_link_test_4() ->
    Predicate = {ydb_cv, num, 'ne', 83}
  , test_setup(Predicate, 46)
.

start_link_test_5() ->
    Predicate = {ydb_cv, num, 'gte', 60}
  , test_setup(Predicate, 32)
.

start_link_test_6() ->
    Predicate = {ydb_cv, num, 'lte', 37}
  , test_setup(Predicate, 7)
.

test_setup(Predicate, NumResults) ->
    Schema = [{num, {1, int}}]
    % Read from the file
  , {ok, InPid} = ydb_file_input:start_link([
        {filename, "../data/select_test_helper.dta"}
      , {batch_size, 50}
      , {poke_freq, 1}
    ], [{schema, Schema}])
    % The select node
  , {ok, SelectPid} = ydb_select:start_link([
        {predicate, Predicate}
    ], [{listen, [InPid]}])
  , ydb_plan_node:add_listener(InPid, SelectPid)
  , Listener = spawn(?MODULE, start_link_test_helper, [self(), 0, NumResults])
  , ydb_plan_node:add_listener(SelectPid, Listener)
  , ydb_file_input:do_read(InPid)
  , receive
        test_passed -> true
      ; fail -> false
    end
.

start_link_test_helper(Pid, Count, NumResults) when Count == NumResults ->
    Pid ! test_passed
;

start_link_test_helper(Pid, Count, NumResults) ->
    receive
        {tuple, {ydb_tuple, _Timestamp, _Data}} ->
            start_link_test_helper(Pid, Count + 1, NumResults)
      ; _Other -> Pid ! fail
    end
.
