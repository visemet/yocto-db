%% @author Angela Gong <anjoola@anjoola.com>

%% @doc This module test the COUNT aggregate function for streams.
-module(ydb_count_test).
-export([handle_results/3]).

-include_lib("eunit/include/eunit.hrl").

start_link_test() ->
    ?assert(start_link_test_helper(50, 13, 13, true))
  , ?assert(start_link_test_helper(60, 18, 18, true))
  , ?assert(start_link_test_helper(40, 8, 8, true))
  , ?assert(start_link_test_helper(90, 40, 40, true))
  , ?assert(start_link_test_helper(96, 48, 48, true))
  , ?assert(start_link_test_helper(50, 13, 13, false))
  , ?assert(start_link_test_helper(60, 18, 18, false))
  , ?assert(start_link_test_helper(40, 8, 8, false))
  , ?assert(start_link_test_helper(90, 40, 40, false))
  , ?assert(start_link_test_helper(96, 48, 48, false))
.

start_link_test_helper(LTValue, NumResults, Answer, Incremental) ->
    Predicate = {ydb_cv, num, 'lt', LTValue}
  , test_setup(Predicate, NumResults, Answer, Incremental)
.

test_setup(Predicate, NumResults, Answer, Incremental) ->
    Schema = [{num, {1, int}}]
    % Read from the file.
  , {ok, InPid} = ydb_file_input:start_link([
        {filename, "../test/data/select_test_helper.dta"}
      , {batch_size, 100}
      , {poke_freq, 1}
    ], [{schema, Schema}])
  , {ok, SelectPid} = ydb_select:start_link([
        {predicate, Predicate}
    ], [{listen, [InPid]}])
    
    % Get the aggregate functions.
  , {PrFun, AggrFun} = ydb_aggr_funs:get_aggr([
        {incremental, Incremental}
      , {name, count}
      , {private, false}
    ])
    
    % The aggregate setup.
  , {ok, AggrPid} = ydb_aggr_node:start_link([
        {incremental, Incremental}
      , {columns, [num]}
      , {history_size, 'infinity'}
      , {result_name, 'COUNT(num)'}
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
            Count = element(1, element(3, lists:nth(NumResults, Tuples)))
          , if
                Count == Answer -> Pid ! test_passed
              ; true -> Pid ! fail
            end
      ; _Other -> Pid ! fail
    end
.