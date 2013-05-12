%% @author Angela Gong <anjoola@anjoola.com>

%% @doc This module tests the VAR aggregate for streams.
-module(ydb_var_test).
-export([handle_results/3]).

-include_lib("eunit/include/eunit.hrl").

start_link_test() ->
    ?assert(start_link_test_helper(50, 13, 20.82840236))
  , ?assert(start_link_test_helper(60, 18, 77.58024691))
  , ?assert(start_link_test_helper(40, 8, 4.484374998))
  , ?assert(start_link_test_helper(90, 40, 321.4774996))
  , ?assert(start_link_test_helper(96, 48, 416.3732636))
.

start_link_test_helper(LTValue, NumResults, Answer) ->
    Predicate = {ydb_cv, num, 'lt', LTValue}
  , test_setup(Predicate, NumResults, Answer)
.

test_setup(Predicate, NumResults, Answer) ->
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
    
    % The aggregate setup.
  , {ok, AggrPid} = ydb_aggr_node:start_link([
        {incremental, true}
      , {columns, [num]}
      , {result_name, 'VAR(num)'}
      , {result_type, float}
      , {eval_fun, fun ydb_aggr_funs:identity/1}
      , {pr_fun, fun ydb_aggr_funs:var_single/2}
      , {aggr_fun, fun ydb_aggr_funs:var_all/1}
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
            Var = element(1, element(3, lists:nth(NumResults, Tuples)))
          , Diff = abs(Var - Answer)
          , if
                Diff =< 0.00001 -> Pid ! test_passed
              ; true -> Pid ! fail
            end
      ; _Other -> Pid ! fail
    end
.