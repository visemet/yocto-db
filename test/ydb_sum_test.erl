%% @author Angela Gong <anjoola@anjoola.com>

%% @doc This module tests the SUM aggregate for streams.
-module(ydb_sum_test).
-export([handle_results/3]).

-include_lib("eunit/include/eunit.hrl").

start_link_test() ->
    ?assert(start_link_test_helper(50, 13, 503, true))
  , ?assert(start_link_test_helper(60, 18, 784, true))
  , ?assert(start_link_test_helper(40, 8, 285, true))
  , ?assert(start_link_test_helper(90, 40, 2434, true))
  , ?assert(start_link_test_helper(96, 48, 3182, true))
  , ?assert(start_link_test_helper(50, 13, 503, false))
  , ?assert(start_link_test_helper(60, 18, 784, false))
  , ?assert(start_link_test_helper(40, 8, 285, false))
  , ?assert(start_link_test_helper(90, 40, 2434, false))
  , ?assert(start_link_test_helper(96, 48, 3182, false))
.

start_link_test_helper(LTValue, NumResults, Answer, Incremental) ->
    Predicate = {ydb_cv, num, 'lt', LTValue}
  , test_setup(Predicate, NumResults, Answer, Incremental)
.

test_setup(Predicate, NumResults, Answer, Incremental) ->
    Schema = [{num, {1, int}}]
    % Read from the file
  , {ok, InPid} = ydb_file_input:start_link([
        {filename, "../data/select_test_helper.dta"}
      , {batch_size, 50}
      , {poke_freq, 1}
    ], [{schema, Schema}])
  , {ok, SelectPid} = ydb_select:start_link([
        {predicate, Predicate}
    ], [{listen, [InPid]}])
    
    % Get the aggregate functions.
  , {PrFun, AggrFun} = ydb_aggr_funs:get_aggr([
        {incremental, Incremental}
      , {name, sum}
      , {private, false}
    ])
  
    % The aggregate setup.
  , {ok, AggrPid} = ydb_aggr_node:start_link([
        {incremental, Incremental}
      , {columns, [num]}
      , {history_size, 'infinity'}
      , {result_name, 'SUM(num)'}
      , {result_type, float}
      , {eval_fun, fun ydb_aggr_funs:identity/1}
      , {pr_fun, PrFun}
      , {aggr_fun, AggrFun}
    ], [{listen, [SelectPid]}])
  , Listener = spawn(?MODULE, handle_results, [self(), NumResults, Answer])
  , ydb_plan_node:add_listener(AggrPid, Listener)
  , ydb_file_input:do_read(InPid)
  
  , receive
        test_passed -> true
      ; fail -> false
    end
.

handle_results(Pid, NumResults, Answer) ->
    receive
        {'$gen_cast', {relegate, {tuples, Tuples}}} ->
            Sum = element(1, element(3, lists:nth(NumResults, Tuples)))
          , if
                Sum == Answer -> Pid ! test_passed
              ; true -> Pid ! fail
            end
      ; _Other -> Pid ! fail
    end
.