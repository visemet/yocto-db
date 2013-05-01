%% @author Angela Gong <anjoola@anjoola.com>

%% @doc This module tests MAX aggregate node.
-module(ydb_max_test).
-export([start_link_test_helper/4]).

-include_lib("eunit/include/eunit.hrl").

start_link_test() ->
    ?assert(start_link_test_helper(50, 13, 49))
  , ?assert(start_link_test_helper(40, 8, 39))
  , ?assert(start_link_test_helper(90, 40, 89))
  , ?assert(start_link_test_helper(96, 48, 95))
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
      , {batch_size, 50}
      , {poke_freq, 1}
    ], [{schema, Schema}])
  , {ok, SelectPid} = ydb_select:start_link([
        {predicate, Predicate}
    ], [{listen, [InPid]}])
    % The aggregate node
  , {ok, AggrPid} = ydb_max:start_link([
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
        {tuple, {ydb_tuple, _Timestamp, {Answer}}} ->
            Pid ! test_passed
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
