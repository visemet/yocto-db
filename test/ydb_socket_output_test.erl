%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc This module tests the socket_input functions.
-module(ydb_socket_output_test).
-export([start_link_test_helper/2, start_link_test_helper_2/2]).

-include_lib("eunit/include/eunit.hrl").

start_link_test() ->
    Address = {127, 0, 0, 1}
  , BasePort = 9000
  , ?assertEqual(ok, start_link_test_1(BasePort + 10, Address))
  , ?assertEqual(ok, start_link_test_2(BasePort + 11, Address))
.

start_link_test_1(PortNo, Address) ->
    create_input_node(PortNo)
  , {ok, SocketPid} = ydb_socket_output:start_link([
        {port_no, PortNo}
      , {address, Address}
      ], []
    )

  , SocketPid !
        {tuples, [{first}, {second}, {3}, {fourth, 4}, {fifth, 5, five}]}
  , handle_messages()
.

start_link_test_2(PortNo, Address) ->
    create_input_node_2(PortNo)
  , {ok, SocketPid} = ydb_socket_output:start_link([
        {port_no, PortNo}
      , {address, Address}
      ], []
    )

  , SocketPid ! {tuples, [{hello}, {max, min}, {12}, {"hi"}, {4, 28, 6}]}
  , handle_messages()
.

start_link_test_helper(Pid, 5) -> Pid ! test_passed;
start_link_test_helper(Pid, Count) ->
    receive
        {tuple, {ydb_tuple, _Timestamp, {first}}} ->
            start_link_test_helper(Pid, Count + 1)
      ; {tuple, {ydb_tuple, _Timestamp, {second}}} ->
            start_link_test_helper(Pid, Count + 1)
      ; {tuple, {ydb_tuple, _Timestamp, {3}}} ->
            start_link_test_helper(Pid, Count + 1)
      ; {tuple, {ydb_tuple, _Timestamp, {fourth, 4}}} ->
            start_link_test_helper(Pid, Count + 1)
      ; {tuple, {ydb_tuple, _Timestamp, {fifth, 5, five}}} ->
            start_link_test_helper(Pid, Count + 1)
      ; _Other -> Pid ! fail
    end
.

start_link_test_helper_2(Pid, 5) -> Pid ! test_passed;
start_link_test_helper_2(Pid, Count) ->
    receive
        {tuple, {ydb_tuple, _Timestamp, {hello}}} ->
            start_link_test_helper_2(Pid, Count + 1)
      ; {tuple, {ydb_tuple, _Timestamp, {max, min}}} ->
            start_link_test_helper_2(Pid, Count + 1)
      ; {tuple, {ydb_tuple, _Timestamp, {12}}} ->
            start_link_test_helper_2(Pid, Count + 1)
      ; {tuple, {ydb_tuple, _Timestamp, {"hi"}}} ->
            start_link_test_helper_2(Pid, Count + 1)
      ; {tuple, {ydb_tuple, _Timestamp, {4, 28, 6}}} ->
            start_link_test_helper_2(Pid, Count + 1)
      ; _Other -> Pid ! fail
    end
.

create_input_node(PortNo) ->
    {ok, PortPid} = ydb_socket_input:start_link([
        {port_no, PortNo}
      ], []
    )
  , Listener = spawn(?MODULE, start_link_test_helper, [self(), 0])
  , ydb_plan_node:add_listener(PortPid, Listener)
.

create_input_node_2(PortNo) ->
    {ok, PortPid} = ydb_socket_input:start_link([
        {port_no, PortNo}
      ], []
    )
  , Listener = spawn(?MODULE, start_link_test_helper_2, [self(), 0])
  , ydb_plan_node:add_listener(PortPid, Listener)
.

handle_messages() ->
    receive
        test_passed -> ok
      ; fail -> fail
    end
.
