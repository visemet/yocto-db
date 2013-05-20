%% @author Angela Gong <anjoola@anjoola.com>

%% @doc This module tests the STDDEV aggregate for streams.
-module(ydb_stddev_test).
-export([handle_results/3]).

-include_lib("eunit/include/eunit.hrl").

start_link_test() ->
    ?assert(start_link_test_helper(50, 13, 4.563814453, true))
  , ?assert(start_link_test_helper(60, 18, 8.807964970, true))
  , ?assert(start_link_test_helper(40, 8, 2.117634293, true))
  , ?assert(start_link_test_helper(90, 40, 17.92979363, true))
  , ?assert(start_link_test_helper(96, 48, 20.40522638, true))
  , ?assert(start_link_test_helper(50, 13, 4.563814453, false))
  , ?assert(start_link_test_helper(60, 18, 8.807964970, false))
  , ?assert(start_link_test_helper(40, 8, 2.117634293, false))
  , ?assert(start_link_test_helper(90, 40, 17.92979363, false))
  , ?assert(start_link_test_helper(96, 48, 20.40522638, false))
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
      , {batch_size, 100}
      , {poke_freq, 1}
    ], [{schema, Schema}])
  , {ok, SelectPid} = ydb_select:start_link([
        {predicate, Predicate}
    ], [{listen, [InPid]}])
    
    % Get the aggregate functions.
  , {PrFun, AggrFun} = ydb_aggr_funs:get_aggr([
        {incremental, Incremental}
      , {name, stddev}
      , {private, false}
    ])
   
    % The aggregate setup.
  , {ok, AggrPid} = ydb_aggr_node:start_link([
        {incremental, Incremental}
      , {columns, [num]}
      , {history_size, 'infinity'}
      , {result_name, 'STDDEV(num)'}
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
            StdDev = element(1, element(3, lists:nth(NumResults, Tuples)))
          , Diff = abs(StdDev - Answer)
          , if
                Diff =< 0.00001 -> Pid ! test_passed
              ; true -> Pid ! fail
            end
      ; _Other -> Pid ! fail
    end
.