%% @author Angela Gong <anjoola@anjoola.com>

%% @doc This module tests the file_output functions.
-module(ydb_file_output_test).
-export([write_test_helper/1]).

-include_lib("eunit/include/eunit.hrl").

write_test() ->
    ?assertEqual(ok, write_test_1("test1.dta"))
  , ?assertEqual(ok, write_test_2("test2.dta")) 
  , ?assertEqual(ok, write_test_3("test3.dta"))
.

write_test_1(Filename) ->
    clear_file(Filename)
  , ydb_file_output:write(Filename, [{first}, {second, 2}, {3}])
  , {ok, InputPid} = ydb_file_input:start_link([
        {filename, Filename}
      , {batch_size, 3}
      , {poke_freq, 10}
      ], []
    )
  , Listener = spawn(?MODULE, write_test_helper, [self()])
  , ydb_plan_node:add_listener(InputPid, Listener)
  , handle_messages(Filename)
.

write_test_2(Filename) ->
    clear_file(Filename)
  , ydb_file_output:write(Filename, [{first}])
  , ydb_file_output:write(Filename, [{second, 2}])
  , ydb_file_output:write(Filename, [{3}])
  , {ok, InputPid} = ydb_file_input:start_link([
        {filename, Filename}
      , {batch_size, 3}
      , {poke_freq, 10}
      ], []
    )
  , Listener = spawn(?MODULE, write_test_helper, [self()])
  , ydb_plan_node:add_listener(InputPid, Listener)
  , handle_messages(Filename)
.

write_test_3(Filename) ->
    clear_file(Filename)
  , ydb_file_output:write(Filename, [{first}, {second, 2}])
  , ydb_file_output:write(Filename, [{3}])
  , {ok, InputPid} = ydb_file_input:start_link([
        {filename, Filename}
      , {batch_size, 3}
      , {poke_freq, 10}
      ], []
    )
  , Listener = spawn(?MODULE, write_test_helper, [self()])
  , ydb_plan_node:add_listener(InputPid, Listener)
  , handle_messages(Filename)
.

write_test_helper(Pid) ->
    receive
        {tuples, [
            {ydb_tuple, _Timestamp1, {first}}
          , {ydb_tuple, _Timestamp2, {second, 2}}
          , {ydb_tuple, _Timestamp3, {3}}
        ]} ->
            Pid ! test_passed
      ; _Other -> Pid ! test_passed
    end
.

clear_file(Filename) ->
    % Clear out the file if necessary.
    IoDevice = file:open(Filename, [write])
  , file:close(IoDevice)
.

handle_messages(Filename) ->
    receive
        test_passed ->
            file:delete(Filename)
          , ok
      ; fail ->
            file:delete(Filename)
          , fail
    end
.
