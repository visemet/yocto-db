%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc This module tests the project node functions on relations.
-module(ydb_project_rel_test).

-export([
    start_link_test_helper/3
]).

-include_lib("eunit/include/eunit.hrl").

start_link_test() ->
    ?assert(start_link_test_1())
  , ?assert(start_link_test_2())
  , ?assert(start_link_test_3())
  , ?assert(start_link_test_4())
  , ?assert(start_link_test_5())
  , ?assert(start_link_test_6())
  , ?assert(start_link_test_7())
  , ?assert(start_link_test_8())
.

start_link_test_1() ->
    Listener = spawn(?MODULE, start_link_test_helper, [self(), 10, 1])
  , test_setup([stock_name], true, Listener)
.

start_link_test_2() ->
    Listener = spawn(?MODULE, start_link_test_helper, [self(), 10, 1])
  , test_setup([day, amt], false, Listener)
.

start_link_test_3() ->
    Listener = spawn(?MODULE, start_link_test_helper, [self(), 10, 2])
  , test_setup([stock_name, day], true, Listener)
.

start_link_test_4() ->
    Listener = spawn(?MODULE, start_link_test_helper, [self(), 10, 2])
  , test_setup([day, amt], true, Listener)
.

start_link_test_5() ->
    Listener = spawn(?MODULE, start_link_test_helper, [self(), 10, 2])
  , test_setup([stock_name, amt], true, Listener)
.

start_link_test_6() ->
    Listener = spawn(?MODULE, start_link_test_helper, [self(), 10, 2])
  , test_setup([day], false, Listener)
.

start_link_test_7() ->
    Listener = spawn(?MODULE, start_link_test_helper, [self(), 10, 3])
  , test_setup([], false, Listener)
.

start_link_test_8() ->
    Listener = spawn(?MODULE, start_link_test_helper, [self(), 10, 0])
  , test_setup([], true, Listener)
.

check_size(Tuples, N) when is_list(Tuples) ->
    lists:foldl(fun(X, Curr) -> check_size(X, N) and Curr end, true, Tuples)
;

check_size({ydb_tuple, _TS, Data}, N) ->
    size(Data) == N
.

start_link_test_helper(Pid, Count, Size) ->
    receive
        {'$gen_cast', {relegate, {diffs, [DiffTid]}}} ->
            ?assertEqual(Count, ets:info(DiffTid, size))
          , {Ins, _Dels} = extract_tuples(DiffTid)
          , ?assert(check_size(Ins, Size))
          , Pid ! test_passed
      ; _Other -> Pid ! fail
    end
.

extract_tuples(DiffTid) ->
    {Ins, Dels} = ydb_ets_utils:extract_diffs([DiffTid])
  , {lists:sort(Ins), lists:sort(Dels)}
.

test_setup(Columns, Include, Listener) ->
    Schema = [{stock_name, {1, string}}, {day, {2, int}}, {amt, {3, int}}]
  , {DiffTid} = get_diff_tables()

    % Read from the file
  , {ok, DummyPid} = ydb_file_input:start_link([
        {filename, "../data/project_test_helper.dta"}
      , {batch_size, 1}
      , {poke_freq, 1}
    ], [{schema, Schema}])
    % The project node
  , {ok, ProjectPid} = ydb_project:start_link([
        {columns, Columns}
      , {include, Include}
    ], [{listen, [DummyPid]}])

    % Listen for results
  , ydb_plan_node:add_listener(ProjectPid, Listener)

  , ydb_plan_node:send_diffs(DummyPid, [DiffTid])

  , receive
        test_passed -> true
      ; fail -> false
    end
.

get_diff_tables() ->
    Diff1 = ets:new(diff1, [bag])
  , ets:insert(Diff1, {'+', {row, 1}, {ydb_tuple, 1, {bp, 4, 132.2200193882}}})
  , ets:insert(Diff1, {'+', {row, 1}, {ydb_tuple, 2, {aapl, 1, 176.18048677}}})
  , ets:insert(Diff1, {'+', {row, 1}, {ydb_tuple, 3, {aapl, 6, 150.76120702}}})
  , ets:insert(Diff1, {'+', {row, 1}, {ydb_tuple, 4, {aapl, 3, 119.88367623}}})
  , ets:insert(Diff1, {'+', {row, 1}, {ydb_tuple, 5, {bp, 5, 190.1371387872}}})
  , ets:insert(Diff1, {'+', {row, 1}, {ydb_tuple, 6, {msft, 4, 392.51540295}}})
  , ets:insert(Diff1, {'+', {row, 1}, {ydb_tuple, 7, {aapl, 6, 279.12898422}}})
  , ets:insert(Diff1, {'+', {row, 1}, {ydb_tuple, 8, {aapl, 5, 202.07971254}}})
  , ets:insert(Diff1, {'+', {row, 1}, {ydb_tuple, 9, {bp, 6, 180.5081589672}}})
  , ets:insert(Diff1, {'+', {row, 1}, {ydb_tuple, 10, {msft, 3, 152.9964933}}})
  , {Diff1}
.
