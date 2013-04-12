%% @author Angela Gong <anjoola@anjoola.com>

%% @doc This module tests the file_input functions.
-module(ydb_file_input_test).
-export([read_test_4_helper/2, read_test_5_helper/1, read_test_6_helper/2]).

-include_lib("eunit/include/eunit.hrl").

read_test() ->
    ?assertMatch(
        {done, [5,4,3,2,1]}
      , ydb_file_input:read(none, -1, [1,2,3,4,5])
    )
  , ?assertMatch(
        {continue, [7,2,4,6]}
      , ydb_file_input:read(none, 0, [6,4,2,7])
    )
  , ?assertMatch(
        {done, [{first},{second},{third},{fourth},{fifth}]}
      , read_test_1()
    )
  , ?assertMatch(
        {continue, [{first}]}
      , read_test_2()
    )
  , ?assertMatch(
        {done, [{first},{second},{third},{fourth},{fifth}]}
      , read_test_3()
    )
  , ?assertEqual(ok, read_test_4())
  , ?assertEqual(ok, read_test_5())
  , ?assertEqual(ok, read_test_6())
.


read_test_1() ->
    IoDevice = ydb_file_input:open("../data/read_test_helper.dta")
  , ydb_file_input:read(IoDevice, 10)
.

read_test_2() ->
    IoDevice = ydb_file_input:open("../data/read_test_helper.dta")
  , ydb_file_input:read(IoDevice, 1)
.

read_test_3() ->
    IoDevice = ydb_file_input:open("../data/read_test_helper.dta")
  , ydb_file_input:read(IoDevice, 6)
.

read_test_4() ->
    {ok, InputPid} = ydb_file_input:start_link([
        {filename,"../data/read_test_helper.dta"}
      , {batch_size, 3}
      , {poke_freq, 1}
      ], []
    )
  , Listener = spawn(?MODULE, read_test_4_helper, [self(), 0])
  , ydb_plan_node:add_listener(InputPid, Listener)
  , handle_messages()
.

read_test_4_helper(Pid, 2) -> Pid ! test_passed;

read_test_4_helper(Pid, Count) ->
    receive
        {tuples, [
            {ydb_tuple, _Timestamp1, {first}}
          , {ydb_tuple, _Timestamp2, {second}}
          , {ydb_tuple, _Timestamp3, {third}}
        ]} ->
            read_test_4_helper(Pid, Count + 1)
      ; {tuples, [
            {ydb_tuple, _Timestamp4, {fourth}}
          , {ydb_tuple, _Timestamp5, {fifth}}
        ]} ->
            read_test_4_helper(Pid, Count + 1)
      ; _Other -> Pid ! fail
    end
.
    
read_test_5() ->
    {ok, InputPid} = ydb_file_input:start_link([
        {filename,"../data/read_test_helper.dta"}
      , {batch_size, 5}
      , {poke_freq, 1}
      ], []
    )
  , Listener = spawn(?MODULE, read_test_5_helper, [self()])
  , ydb_plan_node:add_listener(InputPid, Listener)
  , handle_messages()
.

read_test_5_helper(Pid) ->
    receive
        {tuples, [
            {ydb_tuple, _Timestamp1, {first}}
          , {ydb_tuple, _Timestamp2, {second}}
          , {ydb_tuple, _Timestamp3, {third}}
          , {ydb_tuple, _Timestamp4, {fourth}}
          , {ydb_tuple, _Timestamp5, {fifth}}
        ]} ->
            Pid ! test_passed
      ; _Other -> Pid ! fail
    end
.

read_test_6() ->
    {ok, InputPid} = ydb_file_input:start_link([
        {filename,"../data/read_test_helper.dta"}
      , {batch_size, 1}
      , {poke_freq, 1}
      ], []
    )
  , Listener = spawn(?MODULE, read_test_6_helper, [self(), 0])
  , ydb_plan_node:add_listener(InputPid, Listener)
  , handle_messages()
.

read_test_6_helper(Pid, 5) -> Pid ! test_passed;

read_test_6_helper(Pid, Count) ->
    receive
        {tuples, [{ydb_tuple, _Timestamp, {first}}]} ->
            read_test_6_helper(Pid, Count + 1)
      ; {tuples, [{ydb_tuple, _Timestamp, {second}}]} ->
            read_test_6_helper(Pid, Count + 1)
      ; {tuples, [{ydb_tuple, _Timestamp, {third}}]} ->
            read_test_6_helper(Pid, Count + 1)
      ; {tuples, [{ydb_tuple, _Timestamp, {fourth}}]} ->
            read_test_6_helper(Pid, Count + 1)
      ; {tuples, [{ydb_tuple, _Timestamp, {fifth}}]} ->
            read_test_6_helper(Pid, Count + 1)
      ; _Other -> Pid ! fail
    end
.

handle_messages() ->
    receive
        test_passed -> ok
      ; fail -> fail
    end
.
