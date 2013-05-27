%% @author Angela Gong <anjoola@anjoola.com>

%% @doc This module tests the private AVG aggregate function. It just
%%      makes sure that the true average is not outputted.
-module(ydb_avg_priv_test).
-export([handle_results/3]).

-include_lib("eunit/include/eunit.hrl").

start_link_test() ->
    ?assert(start_link_test_helper(50, 13, 38.69230769))
  , ?assert(start_link_test_helper(60, 18, 43.5555555556))
  , ?assert(start_link_test_helper(40, 8, 35.6250))
  , ?assert(start_link_test_helper(90, 40, 60.850))
  , ?assert(start_link_test_helper(96, 48, 66.2916667))
.

start_link_test_helper(LTValue, NumResults, Answer) ->
    Predicate = {ydb_cv, num, 'lt', LTValue}
  , test_setup(Predicate, NumResults, Answer)
.                                             

test_setup(Predicate, NumResults, Answer) ->
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
        {incremental, true}
      , {name, avg}
      , {private, true}
    ])
    % Get the evaluation function.
  , EvalFun = ydb_aggr_funs:make_private(
        fun ydb_aggr_funs:identity/1, 0.01, 'binary', 1
    )
    
    % The aggregate setup.
  , {ok, AggrPid} = ydb_aggr_node:start_link([
        {incremental, true}
      , {columns, ['$timestamp', num]}
      , {history_size, 'infinity'}
      , {result_name, 'AVG-PRIV(num)'}
      , {result_type, float}
      , {eval_fun, EvalFun}
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
            Avg = element(1, element(3, lists:nth(NumResults, Tuples)))
          , Diff = abs(Avg - Answer)
          , if
                Diff > 0.01 -> Pid ! test_passed
              ; true -> Pid ! fail
            end
      ; _Other -> Pid ! fail
    end
.