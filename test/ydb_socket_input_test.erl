%% @author Angela Gong <anjoola@anjoola.com>

%% @doc This module tests the socket input functions.
-module(ydb_socket_input_test).
-export([start_link_test_helper/1, start_link_test_helper_2/2]).

-include_lib("eunit/include/eunit.hrl").

start_link_test() ->
    Address = {127, 0, 0, 1}
  , BasePort = 9000
  , ?assertEqual(ok, start_link_test_1(BasePort, Address))
  , ?assertEqual(ok, start_link_test_2(BasePort + 1, Address))
  , ?assertMatch(
        {error, {badarg, _Term}}
      , start_link_test_3(BasePort + 2, Address)
    )
.

start_link_test_1(PortNo, Address) ->
    test_helper(PortNo)
  , ydb_socket_utils:send_tuples(
        Address
      , PortNo
      , [{first}, {second}, {3}, {fourth, 4}, {fifth, 5, five}]
    )
  , handle_messages()
.

start_link_test_2(PortNo, Address) ->
    {ok, SocketPid} = ydb_socket_input:start_link([{port_no, PortNo}], [])
  , Listener = spawn(?MODULE, start_link_test_helper_2, [self(), 0])
  , ydb_plan_node:add_listener(SocketPid, Listener)
  
  , ydb_socket_utils:send_tuples(Address, PortNo, [{first}])
  , ydb_socket_utils:send_tuples(Address, PortNo, [{second}])
  , ydb_socket_utils:send_tuples(Address, PortNo, [{3}])
  , ydb_socket_utils:send_tuples(Address, PortNo, [{fourth, 4}])
  , ydb_socket_utils:send_tuples(Address, PortNo, [{fifth, 5, five}])
  , handle_messages()
.

start_link_test_3(PortNo, Address) -> 
    test_helper(PortNo)
  , ydb_socket_utils:send_tuples(Address, PortNo, 3)
.

start_link_test_helper(Pid) ->
    receive
        {'$gen_cast', {relegate, {tuples, [
            {ydb_tuple, _Timestamp1, {first}}
          , {ydb_tuple, _Timestamp2, {second}}
          , {ydb_tuple, _Timestamp3, {3}}
          , {ydb_tuple, _Timestamp4, {fourth, 4}}
          , {ydb_tuple, _Timestamp5, {fifth, 5, five}}
        ]}}} ->
            Pid ! test_passed
      ; _Other -> Pid ! fail
    end
.

start_link_test_helper_2(Pid, 5) -> Pid ! test_passed;

start_link_test_helper_2(Pid, Count) ->
    receive
        {'$gen_cast', {relegate, {tuples,
            [{ydb_tuple, _Timestamp, {first}}]
        }}} ->
            start_link_test_helper_2(Pid, Count + 1)
      ; {'$gen_cast', {relegate, {tuples,
            [{ydb_tuple, _Timestamp, {second}}]
        }}} ->
            start_link_test_helper_2(Pid, Count + 1)
      ; {'$gen_cast', {relegate, {tuples,
            [{ydb_tuple, _Timestamp, {3}}]
        }}} ->
            start_link_test_helper_2(Pid, Count + 1)
      ; {'$gen_cast', {relegate, {tuples,
            [{ydb_tuple, _Timestamp, {fourth, 4}}]
        }}} ->
            start_link_test_helper_2(Pid, Count + 1)
      ; {'$gen_cast', {relegate, {tuples, 
            [{ydb_tuple, _Timestamp, {fifth, 5, five}}]
        }}} ->
            start_link_test_helper_2(Pid, Count + 1)
      ; _Other -> Pid ! fail
    end
.

test_helper(PortNo) ->
    {ok, SocketPid} = ydb_socket_input:start_link([{port_no, PortNo}], [])
  , Listener = spawn(?MODULE, start_link_test_helper, [self()])
  , ydb_plan_node:add_listener(SocketPid, Listener)
.

handle_messages() ->
    receive
        test_passed -> ok
      ; fail -> fail
    end
.
