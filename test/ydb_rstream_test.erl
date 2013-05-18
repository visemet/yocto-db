%% @author Angela Gong <anjoola@anjoola.com>

%% @doc This module tests the rstream module.
-module(ydb_rstream_test).
-export([handler/2]).

-include_lib("eunit/include/eunit.hrl").

start_link_test() ->
    ?assert(rstream_test())
.

rstream_test() ->
    Tids = create_tid()
  , {ok, StreamPid} = ydb_rstream:start_link([], [])
  , Listener = spawn(?MODULE, handler, [self(), 0])
  , ydb_plan_node:add_listener(StreamPid, Listener)
  , StreamPid ! {diffs, Tids}
  , receive
        test_passed -> true
      ; fail -> false
    end
.

handler(Pid, 3) -> Pid ! test_passed;

handler(Pid, Count) ->
    receive
        {'$gen_cast', {relegate, {tuples, Tuples}}} -> case lists:sort(
            fun(A, B) -> element(2, A) < element(2, B) end, Tuples
        ) of
            [
                {ydb_tuple, 1, {first}}
              , {ydb_tuple, 2, {second}}
              , {ydb_tuple, 3, {third}}
            ] ->
                handler(Pid, Count + 1)

          ; [
                {ydb_tuple, 4, {fourth}}
              , {ydb_tuple, 5, {fifth}}
              , {ydb_tuple, 6, {sixth}}
              , {ydb_tuple, 7, {seventh}}
            ] ->
                handler(Pid, Count + 1)

          ; [
                {ydb_tuple, 4, {fourth}}
              , {ydb_tuple, 5, {fifth}}
              , {ydb_tuple, 6, {sixth}}
              , {ydb_tuple, 7, {seventh}}
              , {ydb_tuple, 8, {eigth}}
              , {ydb_tuple, 9, {ninth}}
            ] ->
                handler(Pid, Count + 1)

          ; _Other -> Pid ! fail
        end
    end
.

create_tid() ->
    PreDiff = ets:new(prediff, [bag])
  , ets:insert(PreDiff, {'+', {row, 1}, {ydb_tuple, 1, {first}}})
  , ets:insert(PreDiff, {'+', {row, 2}, {ydb_tuple, 2, {second}}})
  , ets:insert(PreDiff, {'+', {row, 3}, {ydb_tuple, 3, {third}}})

  , Diff = ets:new(diff, [bag])
  , ets:insert(Diff, {'-', {row, 1}, {ydb_tuple, 1, {first}}})
  , ets:insert(Diff, {'-', {row, 2}, {ydb_tuple, 2, {second}}})
  , ets:insert(Diff, {'-', {row, 3}, {ydb_tuple, 3, {third}}})
  , ets:insert(Diff, {'+', {row, 4}, {ydb_tuple, 4, {fourth}}})
  , ets:insert(Diff, {'+', {row, 5}, {ydb_tuple, 5, {fifth}}})
  , ets:insert(Diff, {'+', {row, 6}, {ydb_tuple, 6, {sixth}}})
  , ets:insert(Diff, {'+', {row, 7}, {ydb_tuple, 7, {seventh}}})

  , NextDiff = ets:new(nextdiff, [bag])
  , ets:insert(Diff, {'+', {row, 8}, {ydb_tuple, 8, {eigth}}})
  , ets:insert(Diff, {'+', {row, 9}, {ydb_tuple, 9, {ninth}}})

  , [PreDiff, Diff, NextDiff]
.