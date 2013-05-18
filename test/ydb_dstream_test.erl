%% @author Angela Gong <anjoola@anjoola.com>

%% @doc This module tests the dstream module.
-module(ydb_dstream_test).
-export([handler/1]).

-include_lib("eunit/include/eunit.hrl").

start_link_test() ->
    ?assert(dstream_test())
.

dstream_test() ->
    Tid = create_tid()
  , {ok, StreamPid} = ydb_dstream:start_link([], [])
  , Listener = spawn(?MODULE, handler, [self()])
  , ydb_plan_node:add_listener(StreamPid, Listener)
  , StreamPid ! {diffs, [Tid]}
  , receive
        test_passed -> true
      ; fail -> false
    end
.

handler(Pid) ->
    receive
        {'$gen_cast', {relegate, {tuples, [
            {ydb_tuple, 1, {first}}
          , {ydb_tuple, 2, {second}}
          , {ydb_tuple, 3, {third}}
        ]}}} ->
            Pid ! test_passed
      ; _Other -> Pid ! fail
    end
.

create_tid() ->
    Diff = ets:new(diff, [bag])
  , ets:insert(Diff, {'-', {row, 1}, {ydb_tuple, 1, {first}}})
  , ets:insert(Diff, {'-', {row, 2}, {ydb_tuple, 2, {second}}})
  , ets:insert(Diff, {'-', {row, 3}, {ydb_tuple, 3, {third}}})
  , ets:insert(Diff, {'+', {row, 4}, {ydb_tuple, 4, {fourth}}})
  , ets:insert(Diff, {'+', {row, 5}, {ydb_tuple, 5, {fifth}}})
  , ets:insert(Diff, {'+', {row, 6}, {ydb_tuple, 6, {sixth}}})
  , ets:insert(Diff, {'+', {row, 7}, {ydb_tuple, 7, {seventh}}})
  , Diff
.