%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc This module tests SUM aggregate node for relations.
-module(ydb_sum_rel_test).
-export([start_link_test_helper/4]).

-include_lib("eunit/include/eunit.hrl").

start_link_test() ->
    {Diff1, Diff2} = get_diff_tables()
  , {Out1, Out2, Out12, Out1and2} = get_diff_tuples()
  , ?assert(start_link_test_helper([Diff1], [Out1], 1))
  , ?assert(start_link_test_helper([Diff2], [Out2], 1))
  , ?assert(start_link_test_helper([Diff1, Diff2], [Out1, Out12], 2))
  , ?assert(start_link_test_helper([Diff1, Diff2], Out1and2))
.

% send each tid in a separate message
start_link_test_helper(DiffTids, Outputs, N) ->
    Answer = dict:from_list(lists:zipwith(
        fun(Num, Rows) -> {Num, Rows} end, lists:seq(0, N-1), Outputs))
  , test_setup(DiffTids, Answer, N)
.

% send all the tids in one message
start_link_test_helper(DiffTids, Output) ->
    Answer = dict:from_list([{0, Output}])
  , test_setup2(DiffTids, Answer, 1)
.

test_setup(DiffTids, Answer, N) ->
    AggrPid = test_setup_helper(Answer, N)
  , lists:foreach(fun(X) -> AggrPid ! {diffs, [X]} end, DiffTids)
  , receive
        test_passed -> true
      ; fail -> false
    end
.

test_setup2(DiffTids, Answer, N) ->
    AggrPid = test_setup_helper(Answer, N)
  , AggrPid ! {diffs, DiffTids}
  , receive
        test_passed -> true
      ; fail -> false
    end
.

test_setup_helper(Answer, N) ->
    Schema = [{num, {1, int}}]

    % Set up a dummy node to pass the schema over.
  , {ok, DummyPid} = ydb_file_input:start_link([
        {filename, "../data/select_test_helper.dta"}
      , {batch_size, 50}
      , {poke_freq, 1}
    ], [{schema, Schema}])

    % The aggregate node
  , {ok, AggrPid} = ydb_sum_rel:start_link([
        {column, num}
    ], [{listen, [DummyPid]}])
  , Listener = spawn(
        ?MODULE
      , start_link_test_helper
      , [self(), 0, N, Answer]
    )
  , ydb_plan_node:add_listener(AggrPid, Listener)
  , AggrPid
.

start_link_test_helper(Pid, Count, NumResults, Answer)
  when Count == NumResults - 1 ->
    receive
        {diffs, [DiffTid]} ->
            {ok, Exp} = dict:find(Count, Answer)
          , ?assertEqual(Exp, extract_tuples(DiffTid))
          , Pid ! test_passed
      ; _Other -> Pid ! fail
    end
;

start_link_test_helper(Pid, Count, NumResults, Answer) ->
    receive
        {diffs, [DiffTid]} ->
            {ok, Exp} = dict:find(Count, Answer)
          , ?assertEqual(Exp, extract_tuples(DiffTid))
          , start_link_test_helper(Pid, Count + 1, NumResults, Answer)
      ; _Other -> Pid ! fail
    end
.

extract_tuples(DiffTid) ->
    {Ins, Dels} = ydb_ets_utils:extract_diffs([DiffTid])
  , {lists:sort(Ins), lists:sort(Dels)}
.


% expected output from ydb_ets_utils:extract_diffs/1
get_diff_tuples() ->
    % {[InsertTuples], [DeleteTuples]}
    % there's probably only one of each.
    Table1 = {[{ydb_tuple,4,{15}}],[{ydb_tuple,0,{0}}]}
  , Table2 = {[{ydb_tuple,2,{-2}}],[{ydb_tuple,0,{0}}]}
  , Table12 = {[{ydb_tuple,2,{13}}],[{ydb_tuple,4,{15}}]}
  , Table1and2 = {[{ydb_tuple,4,{13}}],[{ydb_tuple,0,{0}}]}
  , {Table1, Table2, Table12, Table1and2}
.

get_diff_tables() ->
    Diff1 = ets:new(diff1, [bag])
  , ets:insert(Diff1, {'+', {row, 4}, {ydb_tuple, 4, {6}}})
  , ets:insert(Diff1, {'+', {row, 4}, {ydb_tuple, 4, {7}}})
  , ets:insert(Diff1, {'+', {row, 2}, {ydb_tuple, 2, {2}}})

  , Diff2 = ets:new(diff2, [bag])
  , ets:insert(Diff2, {'-', {row, 2}, {ydb_tuple, 2, {2}}})
  , {Diff1, Diff2}
.
