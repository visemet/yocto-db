%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc This module tests the socket output functions.
-module(ydb_socket_output_test).
-export([start_link_test_helper/1, start_link_test_helper_2/1]).

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

  , SocketPid ! {tuples, 
        [{first}, {second}, {3}, {fourth, 4}, {fifth, 5, five}]
    }
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

start_link_test_helper_2(Pid) ->
    receive
        {'$gen_cast', {relegate, {tuples, [
            {ydb_tuple, _Timestamp1, {hello}}
          , {ydb_tuple, _Timestamp2, {max, min}}
          , {ydb_tuple, _Timestamp3, {12}}
          , {ydb_tuple, _Timestamp4, {"hi"}}
          , {ydb_tuple, _Timestamp5, {4, 28, 6}}
        ]}}} ->
            Pid ! test_passed
      ; _Other -> Pid ! fail
    end
.

create_input_node(PortNo) ->
    {ok, PortPid} = ydb_socket_input:start_link([
        {port_no, PortNo}
      ], []
    )
  , Listener = spawn(?MODULE, start_link_test_helper, [self()])
  , ydb_plan_node:add_listener(PortPid, Listener)
.

create_input_node_2(PortNo) ->
    {ok, PortPid} = ydb_socket_input:start_link([
        {port_no, PortNo}
      ], []
    )
  , Listener = spawn(?MODULE, start_link_test_helper_2, [self()])
  , ydb_plan_node:add_listener(PortPid, Listener)
.

handle_messages() ->
    receive
        test_passed -> ok
      ; fail -> fail
    end
.
