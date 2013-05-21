%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc This module tests the MAX aggregate function for relations.
-module(ydb_max_rel_test).
-export([start_link_test_helper/4]).
%-export([handle_results/3]).

-include_lib("eunit/include/eunit.hrl").
-include("ydb_plan_node.hrl").

start_link_test() ->
    {Diff1, Diff2, Diff3} = get_diff_tables()
  , {Out1, Out12, Out123} = get_diff_tuples()
  , ?assert(start_link_test_helper([Diff1], [Out1], 1))
  , ?assert(start_link_test_helper([Diff1, Diff2], [Out1, Out12], 2))
  , ?assert(start_link_test_helper(
        [Diff1, Diff2, Diff3], [Out1, Out12, Out123], 3))
.

% send each tid in a separate message
start_link_test_helper(DiffTids, Outputs, N) ->
    Answer = dict:from_list(lists:zipwith(
        fun(Num, Rows) -> {Num, Rows} end, lists:seq(0, N-1), Outputs))
  , test_setup(DiffTids, Answer, N)
.

test_setup(DiffTids, Answer, N) ->
    DummyPid = test_setup_helper(Answer, N)
  , lists:foreach(
        fun(X) -> ydb_plan_node:send_diffs(DummyPid, [X]) end, DiffTids)
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

    % Get the aggregate functions.
  , {PrFun, AggrFun} = ydb_aggr_funs:get_aggr([
        {incremental, false}
      , {name, max}
      , {private, false}
    ])

    % The aggregate setup.
  , {ok, AggrPid} = ydb_aggr_node:start_link([
        {incremental, false}
      , {columns, [num]}
      , {result_name, 'MAX(num)'}
      , {result_type, float}
      , {eval_fun, fun ydb_aggr_funs:identity/1}
      , {pr_fun, PrFun}
      , {aggr_fun, AggrFun}
      , {history_size, 1}
    ], [{listen, [DummyPid]}])

  , Listener = spawn(
        ?MODULE
      , start_link_test_helper
      , [self(), 0, N, Answer]
    )
  , ydb_plan_node:add_listener(AggrPid, Listener)
  , DummyPid %AggrPid
.

start_link_test_helper(Pid, Count, NumResults, Answer)
  when Count == NumResults - 1 ->
    receive
        {'$gen_cast', {relegate, {diffs, [DiffTid]}}} ->
            {ok, Exp} = dict:find(Count, Answer)
          , ?assertEqual(Exp, extract_tuples(DiffTid))
          , Pid ! test_passed
      ; _Other -> Pid ! fail
    end
;

start_link_test_helper(Pid, Count, NumResults, Answer) ->
    receive
        {'$gen_cast', {relegate, {diffs, [DiffTid]}}} ->
            {ok, Exp} = dict:find(Count, Answer)
          , ?assertEqual(Exp, extract_tuples(DiffTid))
          , start_link_test_helper(Pid, Count + 1, NumResults, Answer)
      ; _Other -> Pid ! fail
    end
.

extract_tuples(DiffTid) ->
    {Ins, Dels} = ydb_ets_utils:extract_diffs([DiffTid])
  , {lists:sort(rnd(Ins)), lists:sort(rnd(Dels))}
.

rnd(Tuples) when is_list(Tuples) ->
    lists:map(fun(X) -> rnd(X) end, Tuples)
;
rnd(Tuple=#ydb_tuple{data=Data}) ->
    {Val} = Data
  , Tuple#ydb_tuple{data={round(Val)}}
.

% expected output from ydb_ets_utils:extract_diffs/1
get_diff_tuples() ->
    % {[InsertTuples], [DeleteTuples]}
    % there's probably only one of each.
    Table1 = {[{ydb_tuple,2,{9}}],[]}
  , Table2 = {[{ydb_tuple,3,{9}}],[{ydb_tuple,2,{9}}]}
  , Table3 = {[{ydb_tuple,4,{7}}],[{ydb_tuple,3,{9}}]}
  , {Table1, Table2, Table3}
.

get_diff_tables() ->
    Diff1 = ets:new(diff1, [bag])
  , ets:insert(Diff1, {'+', {row, 1}, {ydb_tuple, 1, {3}}})
  , ets:insert(Diff1, {'+', {row, 2}, {ydb_tuple, 2, {9}}})

  , Diff2 = ets:new(diff2, [bag])
  , ets:insert(Diff2, {'+', {row, 3}, {ydb_tuple, 3, {5}}})

  , Diff3 = ets:new(diff3, [bag])
  , ets:insert(Diff3, {'-', {row, 1}, {ydb_tuple, 1, {3}}})
  , ets:insert(Diff3, {'-', {row, 2}, {ydb_tuple, 2, {9}}})
  , ets:insert(Diff3, {'+', {row, 4}, {ydb_tuple, 4, {7}}})

  , {Diff1, Diff2, Diff3}
.