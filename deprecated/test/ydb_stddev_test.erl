%% @author Angela Gong <anjoola@anjoola.com>

%% @doc This module tests STDDEV aggregate node.
-module(ydb_stddev_test).
-export([start_link_test_helper/4]).

-include_lib("eunit/include/eunit.hrl").

start_link_test() ->
    ?assert(start_link_test_helper(50, 13, 4.750168688))
  , ?assert(start_link_test_helper(60, 18, 9.063321182))
  , ?assert(start_link_test_helper(40, 8, 2.263846285))
  , ?assert(start_link_test_helper(90, 40, 18.15820786))
  , ?assert(start_link_test_helper(96, 48, 20.62116072))
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
    % The aggregate node
  , {ok, AggrPid} = ydb_stddev:start_link([
        {column, num}
    ], [{listen, [SelectPid]}])
  , Listener = spawn(
        ?MODULE
      , start_link_test_helper
      , [self(), 0, NumResults, Answer]
    )
  , ydb_plan_node:add_listener(AggrPid, Listener)
  , ydb_file_input:do_read(InPid)
  , receive
        test_passed -> true
      ; fail -> false
    end
.

start_link_test_helper(Pid, Count, NumResults, Answer)
  when Count == NumResults - 1 ->
    receive
        {tuple, {ydb_tuple, _Timestamp, {StdDev}}} ->
          io:format("~w~n~n", [StdDev]),
            Diff = abs(StdDev - Answer)
          , if
                Diff =< 0.00001 -> Pid ! test_passed
              ; true -> Pid ! fail
            end
      ; _Other -> Pid ! fail
    end
;

start_link_test_helper(Pid, Count, NumResults, Answer) ->
    receive
        {tuple, {ydb_tuple, _Timestamp, _Data}} ->
            start_link_test_helper(Pid, Count + 1, NumResults, Answer)
      ; _Other -> Pid ! fail
    end
.
