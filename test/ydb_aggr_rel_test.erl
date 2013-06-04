%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc This module tests grouping and aggregation on relations.
-module(ydb_aggr_rel_test).
-export([handler/4]).

-include_lib("eunit/include/eunit.hrl").

start_link_test() ->
    ?assert(test_setup(count, [5, 3, 1, 2], [aapl, bp, goog, msft]))
  , ?assert(test_setup(sum, [25, 12, 10, 18], [aapl, bp, goog, msft]))
  , ?assert(test_setup(avg, [5, 4, 10, 9], [aapl, bp, goog, msft]))
  , ?assert(test_setup(min, [2, 1, 10, 6], [aapl, bp, goog, msft]))
  , ?assert(test_setup(max, [8, 9, 10, 12], [aapl, bp, goog, msft]))
  , ?assert(test_setup(var, [6.4, 12.66667, 9],[aapl, bp, msft]))
  , ?assert(test_setup(stddev, [2.52982, 3.55903, 3], [aapl, bp, msft]))
.

handler(Pid, Answers, Groups, Count) ->
    receive
        {'$gen_cast', {relegate, {diffs, [DiffTid]}}} ->
            ?assertEqual(Count, ets:info(DiffTid, size))
          , {Ins, _Dels} = extract_tuples(DiffTid)
          , check_answers(Ins, Answers, Groups)
          , Pid ! test_passed
      ; _Other -> Pid ! fail
    end
.

test_setup(Aggregate, Answers, Groups) ->
    Schema = [{stock, {1, string}}, {days, {2, int}}, {price, {3, double}}]
  , {DiffTid} = get_diff_tables()

    % Read from the file
  , {ok, DummyPid} = ydb_file_input:start_link([
        {filename, "../test/data/project_test_helper.dta"}
      , {batch_size, 1}
      , {poke_freq, 1}
    ], [{schema, Schema}])

    % Get the aggregate functions.
  , {PrFun, AggrFun} = ydb_aggr_funs:get_aggr([
        {incremental, false}
      , {name, Aggregate}
      , {private, false}
    ])

    % The aggregate setup.
  , {ok, AggrPid} = ydb_aggr_node:start_link([
        {incremental, false}
      , {grouped, [stock]}
      , {columns, [price]}
      , {result_name, 'Aggregate'}
      , {result_type, float}
      , {eval_fun, fun ydb_aggr_funs:identity/1}
      , {pr_fun, PrFun}
      , {aggr_fun, AggrFun}
    ], [{listen, [DummyPid]}])

  , Listener = spawn(?MODULE, handler, [self(), Answers, Groups, 4])
  , ydb_plan_node:add_listener(AggrPid, Listener)
  , ydb_plan_node:send_diffs(DummyPid, [DiffTid])

  , receive
        test_passed -> true
      ; fail -> false
    end
.

check_answers(Tuples, Answers, Groups) when is_list(Tuples) ->
    Results = lists:map(fun(X) -> find_group(X, Tuples) end, Groups)
  , ?assertEqual(rnd(Answers), rnd(Results))
.

find_group(Name, [{ydb_tuple,_TS,{Group,Val}} | _Rst]) when Name == Group ->
    Val
;

find_group(Name, [_Hd | Rst]) ->
    find_group(Name, Rst)
.

rnd(Tuples) ->
    lists:map(fun(X) -> round(X*1000)/1000 end, Tuples)
.

extract_tuples(DiffTid) ->
    {Ins, Dels} = ydb_ets_utils:extract_diffs([DiffTid])
  , {lists:sort(Ins), lists:sort(Dels)}
.

get_diff_tables() ->
    Diff1 = ets:new(diff1, [bag])
  , ets:insert(Diff1, {'+', {row, 1}, {ydb_tuple, 4, {bp, 4, 1}}})
  , ets:insert(Diff1, {'+', {row, 1}, {ydb_tuple, 1, {aapl, 1, 2}}})
  , ets:insert(Diff1, {'+', {row, 1}, {ydb_tuple, 6, {aapl, 6, 3}}})
  , ets:insert(Diff1, {'+', {row, 1}, {ydb_tuple, 3, {aapl, 3, 4}}})
  , ets:insert(Diff1, {'+', {row, 1}, {ydb_tuple, 5, {bp, 5, 2}}})
  , ets:insert(Diff1, {'+', {row, 1}, {ydb_tuple, 4, {msft, 4, 6}}})
  , ets:insert(Diff1, {'+', {row, 1}, {ydb_tuple, 6, {aapl, 6, 8}}})
  , ets:insert(Diff1, {'+', {row, 1}, {ydb_tuple, 5, {aapl, 5, 8}}})
  , ets:insert(Diff1, {'+', {row, 1}, {ydb_tuple, 6, {bp, 6, 9}}})
  , ets:insert(Diff1, {'+', {row, 1}, {ydb_tuple, 4, {goog, 4, 10}}})
  , ets:insert(Diff1, {'+', {row, 1}, {ydb_tuple, 3, {msft, 3, 12}}})
  , {Diff1}
.