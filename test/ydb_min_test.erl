%% @author Angela Gong <anjoola@anjoola.com>

%% @doc This module tests the MIN aggregate function for streams.
-module(ydb_min_test).
-export([handle_results/3]).

-include_lib("eunit/include/eunit.hrl").

start_link_test() ->
    ?assert(start_link_test_helper(50, 37, 53, true))
  , ?assert(start_link_test_helper(1, 50, 32, true))
  , ?assert(start_link_test_helper(40, 42, 41, true))
  , ?assert(start_link_test_helper(90, 9, 92, true))
  , ?assert(start_link_test_helper(96, 2, 97, true))
  , ?assert(start_link_test_helper(50, 37, 53, false))
  , ?assert(start_link_test_helper(1, 50, 32, false))
  , ?assert(start_link_test_helper(40, 42, 41, false))
  , ?assert(start_link_test_helper(90, 9, 92, false))
  , ?assert(start_link_test_helper(96, 2, 97, false))
.

start_link_test_helper(GTValue, NumResults, Answer, Incremental) ->
    Predicate = {ydb_cv, num, 'gt', GTValue}
  , test_setup(Predicate, NumResults, Answer, Incremental)
.

test_setup(Predicate, NumResults, Answer, Incremental) ->
    Schema = [{num, {1, int}}]
    % Read from the file
  , {ok, InPid} = ydb_file_input:start_link([
        {filename, "../test/data/select_test_helper.dta"}
      , {batch_size, 50}
      , {poke_freq, 1}
    ], [{schema, Schema}])
  , {ok, SelectPid} = ydb_select:start_link([
        {predicate, Predicate}
    ], [{listen, [InPid]}])
    
    % Get the aggregate functions.
  , {PrFun, AggrFun} = ydb_aggr_funs:get_aggr([
        {incremental, Incremental}
      , {name, min}
      , {private, false}
    ])
    
    % The aggregate setup.
  , {ok, AggrPid} = ydb_aggr_node:start_link([
        {incremental, Incremental}
      , {columns, [num]}
      , {history_size, 'infinity'}
      , {result_name, 'MIN(num)'}
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
            Min = element(1, element(3, lists:nth(NumResults, Tuples)))
          , if
                Min == Answer -> Pid ! test_passed
              ; true -> Pid ! fail
            end
      ; _Other -> Pid ! fail
    end
.