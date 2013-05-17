%% @author Angela Gong <anjoola@anjoola.com>

%% @doc This module tests grouping and aggregation.
-module(ydb_aggr_test).
-export([handler/3]).

-include_lib("eunit/include/eunit.hrl").

start_link_test() ->
    ?assert(test_setup(count, {30, 21, 23, 26}))
  , ?assert(test_setup(sum, {7133.780355, 5163.190019, 6162.32111, 6597.23604}))
  , ?assert(test_setup(avg, {237.792678, 245.866191, 267.927004, 253.739847}))
  , ?assert(test_setup(min, {108.967541, 102.487647, 102.1289392, 106.9148703}))
  , ?assert(test_setup(max, {369.484968, 371.206596, 396.1867941, 386.610781}))
  , ?assert(test_setup(var, {7256.37307, 6475.612582, 8447.03202, 8005.78866}))
  , ?assert(test_setup(stddev, {85.1843475, 80.471191, 91.9077364, 89.4750728}))
.

handler(Pid, _Answers, 4) -> Pid ! test_passed;

handler(Pid, Answers, Count) ->
    receive
        {'$gen_cast', {relegate, {tuples,
            Tuples=[{ydb_tuple, _, {aapl, _}} | _]
        }}} ->
            Result = element(2, element(3, lists:nth(length(Tuples), Tuples)))
          , if
                abs(Result - element(1, Answers)) < 0.0001 ->
                    handler(Pid, Answers, Count + 1)
              ; true -> io:format("AAPL: ~w~n", [Result]), Pid ! fail
            end
      ; {'$gen_cast', {relegate, {tuples,
            Tuples=[{ydb_tuple, _, {goog,  _}} | _]
        }}} ->
            Result = element(2, element(3, lists:nth(length(Tuples), Tuples)))
          , if
                abs(Result - element(2, Answers)) < 0.0001 ->
                    handler(Pid, Answers, Count + 1)
              ; true -> io:format("GOOG: ~w~n", [Result]), Pid ! fail
            end
      ; {'$gen_cast', {relegate, {tuples,
            Tuples=[{ydb_tuple, _, {msft,  _}} | _]
        }}} ->
            Result = element(2, element(3, lists:nth(length(Tuples), Tuples)))
          , if
                abs(Result - element(3, Answers)) < 0.0001 ->
                    handler(Pid, Answers, Count + 1)
              ; true -> io:format("MSFT: ~w~n", [Result]), Pid ! fail
            end
      ; {'$gen_cast', {relegate, {tuples,
            Tuples=[{ydb_tuple, _, {bp, _}} | _]
        }}} ->
            Result = element(2, element(3, lists:nth(length(Tuples), Tuples)))
          , if
                abs(Result - element(4, Answers)) < 0.0001 ->
                    handler(Pid, Answers, Count + 1)
              ; true -> io:format("BP: ~w~n", [Result]), Pid ! fail
            end
    end
.

test_setup(Aggregate, Answers) ->
    Schema = [{stock, {1, string}}, {days, {2, int}}, {price, {3, double}}]
  , {ok, InPid} = ydb_file_input:start_link([
        {filename, "../data/project_test_helper.dta"}
      , {batch_size, 100}
      , {poke_freq, 1}
    ], [{schema, Schema}])
    
     % Get the aggregate functions.
  , {PrFun, AggrFun} = ydb_aggr_funs:get_aggr([
        {incremental, true}
      , {name, Aggregate}
      , {private, false}
    ])
    
    % The aggregate setup.
  , {ok, AggrPid} = ydb_aggr_node:start_link([
        {incremental, true}
      , {grouped, [stock]}
      , {columns, [price]}
      , {result_name, 'Aggregate'}
      , {result_type, float}
      , {eval_fun, fun ydb_aggr_funs:identity/1}
      , {pr_fun, PrFun}
      , {aggr_fun, AggrFun}
    ], [{listen, [InPid]}])
    
  , Listener = spawn(?MODULE, handler, [self(), Answers, 0])
  , ydb_plan_node:add_listener(AggrPid, Listener)
  , ydb_file_input:do_read(InPid)
  
  , receive
        test_passed -> true
      ; fail -> false
    end
.