%% @author Angela Gong <anjoola@anjoola.com>

%% @doc This module tests the joining module.
-module(ydb_join_test).
-export([handler1/1, handler2/1, handler3/1]).

-include_lib("eunit/include/eunit.hrl").

start_link_test() ->
    ?assert(join_test())
  , ?assert(join_test2())
  , ?assert(join_test3())
.

test_setup(Handler) ->
    Schema = [{num, {1, int}}]
  , Predicate = {ydb_cv, num, 'gt', 0}

  , {ok, DummyPid} = ydb_file_input:start_link([
        {filename, "../test/data/select_test_helper.dta"}
      , {batch_size, 100}
      , {poke_freq, 1}
    ], [{schema, Schema}])

    % Create nodes and join them.
  , {ok, LeftPid} = ydb_select:start_link([
        {predicate, Predicate}
    ], [{listen, [DummyPid]}])
  , {ok, RightPid} = ydb_select:start_link([
        {predicate, Predicate}
    ], [{listen, [DummyPid]}])
  , {ok, JoinPid} = ydb_join:start_link([
       {left, {size, {row, 2}}, {pulse, {row, 1}}}
     , {right, {size, {row, 2}}, {pulse, {row, 1}}}
    ], [{listen, [LeftPid, RightPid]}])
    % Listener on the join node.
  , Listener = spawn(?MODULE, Handler, [self()])
  , ydb_plan_node:add_listener(JoinPid, Listener)
  , {LeftPid, RightPid}
.

join_test() ->
    {LeftPid, RightPid} = test_setup(handler1)

  , LeftDiff = ets:new(ldiff, [bag])
  , ets:insert(LeftDiff, {'+', {row, 1}, {ydb_tuple, 1, {1}}})
  , ets:insert(LeftDiff, {'+', {row, 2}, {ydb_tuple, 2, {35}}})
  , ets:insert(LeftDiff, {'+', {row, 3}, {ydb_tuple, 3, {23}}})

  , RightDiff = ets:new(rdiff, [bag])
  , ets:insert(RightDiff, {'+', {row, 1}, {ydb_tuple, 1, {1231}}})
  , ets:insert(RightDiff, {'+', {row, 2}, {ydb_tuple, 2, {15}}})
  , ets:insert(RightDiff, {'+', {row, 3}, {ydb_tuple, 3, {31}}})

  , LeftPid ! {diffs, [LeftDiff]}
  , RightPid ! {diffs, [RightDiff]}
  , handle_messages()
.

join_test2() ->
    {LeftPid, RightPid} = test_setup(handler2)

  , LeftDiff1 = ets:new(ldiff1, [bag])
  , ets:insert(LeftDiff1, {'+', {row, 1}, {ydb_tuple, 1, {1}}})
  , LeftDiff2 = ets:new(ldiff2, [bag])
  , ets:insert(LeftDiff2, {'+', {row, 2}, {ydb_tuple, 2, {35}}})
  , RightDiff = ets:new(rdiff, [bag])
  , ets:insert(RightDiff, {'+', {row, 1}, {ydb_tuple, 1, {1231}}})
  
  , LeftPid ! {diffs, [LeftDiff1, LeftDiff2]}
  , RightPid ! {diffs, [RightDiff]}
  , handle_messages()
.

join_test3() ->
    {LeftPid, RightPid} = test_setup(handler3)

  , LeftDiff1 = ets:new(ldiff1, [bag])
  , ets:insert(LeftDiff1, {'+', {row, 1}, {ydb_tuple, 1, {1}}})
  , RightDiff1 = ets:new(rdiff1, [bag])
  , ets:insert(RightDiff1, {'+', {row, 3}, {ydb_tuple, 3, {342}}})

  , LeftPid ! {diffs, [LeftDiff1]}
  , RightPid ! {diffs, [RightDiff1]}

  , LeftDiff2 = ets:new(ldiff2, [bag])
  , ets:insert(LeftDiff2, {'+', {row, 5}, {ydb_tuple, 5, {1342}}})
  , RightDiff2 = ets:new(rdiff2, [bag])
  , ets:insert(RightDiff2, {'+', {row, 14}, {ydb_tuple, 14, {932}}})

  , RightPid ! {diffs, [RightDiff2]}
  , LeftPid ! {diffs, [LeftDiff2]}
  , handle_messages()
.

handler1(Pid) ->
    receive
        {diffs, [DiffTid]} ->
            {Ins, _Dels} = extract_tuples(DiffTid)
          , erlang:self() ! Ins
          , handler1(Pid)
      ; [{ydb_tuple, _, {1, 15}}, {ydb_tuple, _, {1, 31}},
         {ydb_tuple, _, {1, 1231}}, {ydb_tuple, _, {23, 15}},
         {ydb_tuple, _, {23, 31}}, {ydb_tuple, _, {23, 1231}},
         {ydb_tuple, _, {35, 15}}, {ydb_tuple, _, {35, 31}},
         {ydb_tuple, _, {35, 1231}}] ->
            Pid ! test_passed
      ; _Other -> Pid ! fail
    end
.

handler2(Pid) ->
    receive
        {diffs, [DiffTid]} ->
            {Ins, _Dels} = extract_tuples(DiffTid)
          , erlang:self() ! Ins
          , handler2(Pid)
      ; [{ydb_tuple, _, {1, 1231}}, {ydb_tuple, _, {35, 1231}}] ->
            Pid ! test_passed
      ; _Other -> Pid ! fail
    end
.

handler3(Pid) ->
    receive
        {diffs, [DiffTid]} ->
            {Ins, _Dels} = extract_tuples(DiffTid)
          , erlang:self() ! Ins
          , handler3(Pid)
      ; [{ydb_tuple, _, {1, 342}}] ->
            handler3(Pid)
      ; [{ydb_tuple, _, {1, 932}}, {ydb_tuple, _, {1342, 342}},
         {ydb_tuple, _, {1342, 932}}] ->
            Pid ! test_passed
      ; _Other -> Pid ! fail
    end
.

extract_tuples(DiffTid) ->
    {Ins, Dels} = ydb_ets_utils:extract_diffs([DiffTid])
  , {
        lists:sort(
            fun(A, B) ->
                element(3, A) < element(3, B)
            end
          , Ins
        )
      , lists:sort(
            fun(A, B) ->
                element(3, A) < element(3, B)
            end
          , Dels
        )
    }
.

handle_messages() ->
    receive
        test_passed -> true
      ; fail -> false
    end
.