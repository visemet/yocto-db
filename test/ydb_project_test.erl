%% @author Angela Gong <anjoola@anjoola.com>

%% @doc This module tests the project node functions.
-module(ydb_project_test).
-export([
    start_link_test_helper_0/2
  , start_link_test_helper_1/2
  , start_link_test_helper_2/2
  , start_link_test_helper_3/2
]).

-include_lib("eunit/include/eunit.hrl").

start_link_test() ->
    ?assert(start_link_test_1())
  , ?assert(start_link_test_2())
  , ?assert(start_link_test_3())
  , ?assert(start_link_test_4())
  , ?assert(start_link_test_5())
  , ?assert(start_link_test_6())
  , ?assert(start_link_test_7())
  , ?assert(start_link_test_8())
.

start_link_test_1() ->
    Listener = spawn(?MODULE, start_link_test_helper_1, [self(), 0])
  , test_setup([stock_name], true, Listener)
.

start_link_test_2() ->
    Listener = spawn(?MODULE, start_link_test_helper_1, [self(), 0])
  , test_setup([day, amt], false, Listener)
.

start_link_test_3() ->
    Listener = spawn(?MODULE, start_link_test_helper_2, [self(), 0])
  , test_setup([stock_name, day], true, Listener)
.

start_link_test_4() ->
    Listener = spawn(?MODULE, start_link_test_helper_2, [self(), 0])
  , test_setup([day, amt], true, Listener)
.

start_link_test_5() ->
    Listener = spawn(?MODULE, start_link_test_helper_2, [self(), 0])
  , test_setup([stock_name, amt], true, Listener)
.

start_link_test_6() ->
    Listener = spawn(?MODULE, start_link_test_helper_2, [self(), 0])
  , test_setup([day], false, Listener)
.

start_link_test_7() ->
    Listener = spawn(?MODULE, start_link_test_helper_3, [self(), 0])
  , test_setup([], false, Listener)
.

start_link_test_8() ->
    Listener = spawn(?MODULE, start_link_test_helper_0, [self(), 0])
  , test_setup([], true, Listener)
.

start_link_test_helper_0(Pid, Count) when Count == 100 ->
    Pid ! test_passed
;

start_link_test_helper_0(Pid, Count) ->
    receive
        {tuple, {ydb_tuple, _Timestamp, {}}} ->
            start_link_test_helper_0(Pid, Count + 1)
      ; _Other -> Pid ! fail
    end
.

start_link_test_helper_1(Pid, Count) when Count == 100 ->
    Pid ! test_passed
;

start_link_test_helper_1(Pid, Count) ->
    receive
        {tuple, {ydb_tuple, _Timestamp, {_Col1}}} ->
            start_link_test_helper_1(Pid, Count + 1)
      ; _Other -> Pid ! fail
    end
.

start_link_test_helper_2(Pid, Count) when Count == 100 ->
    Pid ! test_passed
;

start_link_test_helper_2(Pid, Count) ->
    receive
        {tuple, {ydb_tuple, _Timestamp, {_Col1, _Col2}}} ->
            start_link_test_helper_2(Pid, Count + 1)
      ; _Other -> Pid ! fail
    end
.

start_link_test_helper_3(Pid, Count) when Count == 100 ->
    Pid ! test_passed
;

start_link_test_helper_3(Pid, Count) ->
    receive
        {tuple, {ydb_tuple, _Timestamp, {_Col1, _Col2, _Col3}}} ->
            start_link_test_helper_3(Pid, Count + 1)
      ; _Other -> Pid ! fail
    end
.

test_setup(Columns, Include, Listener) ->
    Schema = [{stock_name, {1, string}}, {day, {2, int}}, {amt, {3, int}}]
    % Read from the file
  , {ok, InPid} = ydb_file_input:start_link([
        {filename, "../data/project_test_helper.dta"}
      , {batch_size, 50}
      , {poke_freq, 1}
    ], [{schema, Schema}])
    % The project node
  , {ok, ProjectPid} = ydb_project:start_link([
        {columns, Columns}
      , {include, Include}
    ], [{listen, [InPid]}])
    
    % Listen for results
  , ydb_plan_node:add_listener(ProjectPid, Listener)
  , ydb_file_input:do_read(InPid)
  , receive
        test_passed -> true
      ; fail -> false
    end
.
