%% @author Angela Gong <anjoola@anjoola.com>

%% @doc This module tests VAR aggregate node.
-module(ydb_var_test).
-export([start_link_test_helper/4]).

-include_lib("eunit/include/eunit.hrl").

start_link_test() ->
    ?assert(start_link_test_helper(50, 13, 22.56410256))
  , ?assert(start_link_test_helper(60, 18, 82.14379085))
  , ?assert(start_link_test_helper(40, 8, 5.125000000))
  , ?assert(start_link_test_helper(90, 40, 329.7205128))
  , ?assert(start_link_test_helper(96, 48, 425.2322695))
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
  , {ok, AggrPid} = ydb_var:start_link([
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
        {tuple, {ydb_tuple, _Timestamp, {Var}}} ->
            Diff = abs(Var - Answer)
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
