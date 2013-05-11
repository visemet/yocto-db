%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc This module tests the ets utility functions.
-module(ydb_ets_utils_test).

%% @headerfile "ydb_plan_node.hrl"
-include("include/ydb_plan_node.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% =============================================================== %%%
%%%  Tests                                                          %%%
%%% =============================================================== %%%

create_table_test() ->
    {ok, Tid} = ydb_ets_utils:create_table(test)
  , ?assertEqual([], raw_output(Tid))
.

delete_table_test() ->
    {ok, Tid} = ydb_ets_utils:create_table(test)
  , ?assertEqual(ok, ydb_ets_utils:delete_table(Tid))
  , ?assertEqual(undefined, ets:info(Tid))
.

add_tuples_test() ->
    {ok, Tid} = ydb_ets_utils:create_table(test)
  , {T1, T2, T3, T4, _T5, _T6} = get_tuples(tuple)
  , {S1, S2, S3, S4, _S5, _S6} = get_syn_tuples(tuple)
  , T5 = {ydb_tuple, 5, {i, j}}
  , S5 = {{other,5},{ydb_tuple,5,{i,j}}}
  , T6 = {ydb_tuple, 5, {l, m}}
  , S6 = {{other,5},{ydb_tuple,5,{l, m}}}

    % Adding a single tuple.
  , ydb_ets_utils:add_tuples(Tid, test, T1)
  , ?assertEqual([S1], raw_output(Tid))

    % Adding a singleton list.
  , ydb_ets_utils:add_tuples(Tid, test, [T2])
  , ?assertEqual([S1, S2], raw_output(Tid))

    % Adding a list.
  , ydb_ets_utils:add_tuples(Tid, test, [T3, T4])
  , ?assertEqual([S1, S2, S3, S4], raw_output(Tid))

    % Using a different type.
  , ydb_ets_utils:add_tuples(Tid, other, T5)
  , ?assertEqual(lists:sort([S1, S2, S3, S4, S5]), raw_output(Tid))

    % Adding a second tuple with the same type + timestamp.
  , ydb_ets_utils:add_tuples(Tid, other, T6)
  , ?assertEqual(lists:sort([S1, S2, S3, S4, S5, S6]), raw_output(Tid))
.

delete_tuples_test() ->
    {ok, Tid} = ydb_ets_utils:create_table(test)
  , {ok, Tid2} = ydb_ets_utils:create_table(test)
  , {T1, T2, T3, T4, _T5, _T6} = get_tuples(tuple)
  , {S1, S2, S3, S4, _S5, _S6} = get_syn_tuples(tuple)

    % Test delete_tuples/3.
  , ydb_ets_utils:add_tuples(Tid, test, [T1, T2, T3, T4])
  , ?assertEqual([S1, S2, S3, S4], raw_output(Tid))

  , ydb_ets_utils:delete_tuples(Tid, test, T1)
  , ?assertEqual([S2, S3, S4], raw_output(Tid))

  , ydb_ets_utils:delete_tuples(Tid, test, [T2, T3])
  , ?assertEqual([S4], raw_output(Tid))

  , ydb_ets_utils:delete_tuples(Tid, test, T1)
  , ?assertEqual([S4], raw_output(Tid))

    % Test delete_tuples/2.
  , ydb_ets_utils:add_tuples(Tid2, test, [T1, T2, T3, T4])
  , ?assertEqual([S1, S2, S3, S4], raw_output(Tid2))

  , ydb_ets_utils:delete_tuples(Tid2, {test, 1})
  , ?assertEqual([S2, S3, S4], raw_output(Tid2))

  , ydb_ets_utils:delete_tuples(Tid2, {test, 2})
  , ?assertEqual([S3, S4], raw_output(Tid2))

  , ydb_ets_utils:delete_tuples(Tid2, {test, 1})
  , ?assertEqual([S3, S4], raw_output(Tid2))
.

replace_tuples_test() ->
    {ok, Tid} = ydb_ets_utils:create_table(test)
  , {T1, T2, T3, T4, T5, T6} = get_tuples(tuple)
  , {S1, S2, S3, S4, S5, S6} = get_syn_tuples(tuple)

    % Test add_tuples/3.
  , ydb_ets_utils:add_tuples(Tid, test, [T1, T2, T3, T4])
  , ?assertEqual([S1, S2, S3, S4], raw_output(Tid))

  , ydb_ets_utils:replace_tuple(Tid, test, T1, T5)
  , ?assertEqual([S2, S3, S4, S5], raw_output(Tid))

  , ydb_ets_utils:replace_tuple(Tid, test, T3, T6)
  , ?assertEqual([S2, S4, S5, S6], raw_output(Tid))
.

extract_timestamps_test() ->
    Tid = ets:new(diff1, [bag, {keypos, 2}])
  , ets:insert(Tid, {'+', {row, 1}, {ydb_tuple, 1, {6}}})
  , ets:insert(Tid, {'+', {row, 2}, {ydb_tuple, 2, {7}}})
  , ets:insert(Tid, {'+', {row, 3}, {ydb_tuple, 3, {2}}})
  , ets:insert(Tid, {'-', {row, 10}, {ydb_tuple, 10, {2}}})

  , Timestamps = ydb_ets_utils:extract_timestamps([Tid], 'diff')
  , ?assertEqual([1, 2, 3, 10], lists:sort(Timestamps))

  , {T1, T2, T3, T4, _T5, _T6} = get_tuples(tuple)
  , Tid2 = ets:new(nondiff, [bag])
  , ydb_ets_utils:add_tuples(Tid2, test, [T1, T2, T3, T4])

  , Timestamps2 = ydb_ets_utils:extract_timestamps([Tid2], 'syn')
  , ?assertEqual([1, 2, 3, 4], lists:sort(Timestamps2))

  , Timestamps3 = ydb_ets_utils:extract_timestamps([Tid2], 'rel')
  , ?assertEqual([1, 2, 3, 4], lists:sort(Timestamps3))
.

max_timestamps_test() ->
    Tid = ets:new(diff1, [bag, {keypos, 2}])
  , ets:insert(Tid, {'+', {row, 1}, {ydb_tuple, 1, {6}}})
  , ets:insert(Tid, {'+', {row, 2}, {ydb_tuple, 2, {7}}})
  , ets:insert(Tid, {'+', {row, 3}, {ydb_tuple, 3, {2}}})
  , ets:insert(Tid, {'-', {row, 10}, {ydb_tuple, 10, {2}}})

  , Max = ydb_ets_utils:max_timestamp([Tid], 'diff')
  , ?assertEqual(10, Max)

  , {T1, T2, T3, T4, _T5, _T6} = get_tuples(tuple)
  , Tid2 = ets:new(nondiff, [bag])
  , ydb_ets_utils:add_tuples(Tid2, test, [T1, T2, T3, T4])

  , Max2 = ydb_ets_utils:max_timestamp([Tid2], 'syn')
  , ?assertEqual(4, Max2)

  , Max3 = ydb_ets_utils:max_timestamp([Tid2], 'rel')
  , ?assertEqual(4, Max3)

  , Max4 = ydb_ets_utils:max_timestamp([Tid], 'rel')
  , ?assertEqual(-1, Max4)
.

get_copy_test() ->
    {ok, Tid} = ydb_ets_utils:create_table(test)
  , ets:insert(Tid, get_syn_tuples(list))
  , {ok, Tid2} = ydb_ets_utils:get_copy(Tid, copy)
  , ?assertEqual(get_syn_tuples(list), raw_output(Tid2))
  , ?assertEqual(raw_output(Tid), raw_output(Tid2))
.

combine_partial_results_test() ->
    {S1, S2, S3, S4, S5, S6} = get_syn_tuples(tuple)
  , {ok, Tid} = ydb_ets_utils:create_table(test)
  , {ok, Tid2} = ydb_ets_utils:create_table(test2)
  , ets:insert(Tid, [S1, S2])
  , ets:insert(Tid2, [S3, S4, S5, S6])
  , {ok, Tid3} =
        ydb_ets_utils:combine_partial_results([Tid, Tid2], test3)

  , RawTuples = raw_output(Tid3)
  , ?assertEqual(true, is_rel_tuple(RawTuples))
  , Tuples =
        lists:sort(lists:map(fun({_, Tuple}) -> Tuple end, RawTuples))
  , ?assertEqual(get_tuples(list), Tuples)
.

add_and_combine_results_test() ->
    {T1, T2, T3, T4, T5, T6} = get_tuples(tuple)
  , {ok, Tid} = ydb_ets_utils:create_table(test)
  , {ok, Tid2} = ydb_ets_utils:create_table(test2)
  , ydb_ets_utils:add_tuples(Tid, test, [T1, T2])
  , ydb_ets_utils:add_tuples(Tid2, test, [T3, T4, T5, T6])
  , {ok, Tid3} =
        ydb_ets_utils:combine_partial_results([Tid, Tid2], test3)

  , RawTuples = raw_output(Tid3)
  , ?assertEqual(true, is_rel_tuple(RawTuples))
  , Tuples =
        lists:sort(lists:map(fun({_, Tuple}) -> Tuple end, RawTuples))
  , ?assertEqual(get_tuples(list), Tuples)
.

apply_diffs_test() ->
    {_T1, T2, T3, T4, T5, T6} = get_tuples(tuple)
  , {S1, S2, S3, S4, S5, _S6} = get_syn_tuples(tuple)
  , T7 = {ydb_tuple,7,{m,n}}
  , S7 = {{test, 7}, T7}

  , Base = ets:new(base, [])
  , ets:insert(Base, [S1, S2, S3])

  , Diff1 = ets:new(diff1, [{keypos, 2}])
  , ets:insert(Diff1, get_diff_tuples('-', T2))
  , ets:insert(Diff1, get_diff_tuples('-', T3))
  , ets:insert(Diff1, get_diff_tuples('+', T4))

  , Diff2 = ets:new(diff2, [{keypos, 2}])
  , ets:insert(Diff2, get_diff_tuples('-', T4))
  , ets:insert(Diff2, get_diff_tuples('+', T5))
  , ets:insert(Diff2, get_diff_tuples('+', T6))

  , Diff3 = ets:new(diff3, [{keypos, 2}])
  , ets:insert(Diff3, get_diff_tuples('-', T6))
  , ets:insert(Diff3, get_diff_tuples('+', T7))

  , ydb_ets_utils:apply_diffs(Base, [Diff1])
  , ?assertEqual([S1, S4], raw_output(Base))

  , ydb_ets_utils:apply_diffs(Base, [Diff2, Diff3])
  , ?assertEqual([S1, S5, S7], raw_output(Base))
.

add_diffs_test() ->
    {T1, T2, T3, T4, _T5, _T6} = get_tuples(tuple)
  , {D1, D2, D3, D4} = {
        get_diff_tuples('+', T1)
      , get_diff_tuples('-', T2)
      , get_diff_tuples('+', T3)
      , get_diff_tuples('+', T4)
    }

  , {ok, Diff} = ydb_ets_utils:create_diff_table(diff)
  , ydb_ets_utils:add_diffs(Diff, '+', test, T1)
  , ?assertEqual([D1], raw_output(Diff))

  , ydb_ets_utils:add_diffs(Diff, '-', test, T2)
  , ?assertEqual([D1, D2], raw_output(Diff))

  , ydb_ets_utils:add_diffs(Diff, '+', test, [T3, T4])
  , ?assertEqual(lists:sort([D1, D2, D3, D4]), raw_output(Diff))
.

apply_and_add_diffs_test() ->
    {_T1, T2, T3, T4, T5, T6} = get_tuples(tuple)
  , {S1, S2, S3, _S4, S5, S6} = get_syn_tuples(tuple)

  , Base = ets:new(base, [])
  , ets:insert(Base, [S1, S2, S3])

  , Diff1 = ets:new(diff1, [{keypos, 2}])
  , ydb_ets_utils:add_diffs(Diff1, '-', test, [T2, T3])
  , ydb_ets_utils:add_diffs(Diff1, '+', test, T4)

  , Diff2 = ets:new(diff2, [{keypos, 2}])
  , ydb_ets_utils:add_diffs(Diff2, '+', test, [T5, T6])
  , ydb_ets_utils:add_diffs(Diff2, '-', test, [T4])

  , ydb_ets_utils:apply_diffs(Base, [Diff1, Diff2])
  , ?assertEqual([S1, S5, S6], raw_output(Base))
.

extract_diffs_test() ->
    {T1, T2, T3, T4, T5, T6} = get_tuples(tuple)
  , {D1, D2, D3, D4, D5, D6} = {
        get_diff_tuples('+', T1)
      , get_diff_tuples('-', T2)
      , get_diff_tuples('+', T3)
      , get_diff_tuples('-', T4)
      , get_diff_tuples('-', T5)
      , get_diff_tuples('-', T6)
    }

  , {ok, Diff} = ydb_ets_utils:create_diff_table(diff)
  , ydb_ets_utils:add_diffs(Diff, '+', test, [T1, T3])
  , ydb_ets_utils:add_diffs(Diff, '-', test, [T2, T4, T5, T6])
  , ?assertEqual(lists:sort([D1, D2, D3, D4, D5, D6]), raw_output(Diff))

  , {Ins, Dels} = ydb_ets_utils:extract_diffs([Diff])
  , ?assertEqual(lists:sort([T1, T3]), lists:sort(Ins))
  , ?assertEqual(lists:sort([T2, T4, T5, T6]), lists:sort(Dels))

  , {ok, Diff2} = ydb_ets_utils:create_diff_table(diff2)
  , {ok, Diff3} = ydb_ets_utils:create_diff_table(diff3)
  , ydb_ets_utils:add_diffs(Diff2, '+', test, [T1, T3])
  , ydb_ets_utils:add_diffs(Diff2, '-', test, [T2])
  , ydb_ets_utils:add_diffs(Diff3, '-', test, [T4, T5, T6])

  , {Ins2, Dels2} = ydb_ets_utils:extract_diffs([Diff2, Diff3])
  , ?assertEqual(lists:sort([T1, T3]), lists:sort(Ins2))
  , ?assertEqual(lists:sort([T2, T4, T5, T6]), lists:sort(Dels2))
.

dump_raw_test() ->
    {ok, Tid} = ydb_ets_utils:create_table(test)
  , ets:insert(Tid, get_syn_tuples(list))
  , ?assertEqual(get_syn_tuples(list),
        lists:sort(ydb_ets_utils:dump_raw(Tid)))
.

dump_tuples_test() ->
    {ok, Tid} = ydb_ets_utils:create_table(test)

    % Test dump_tuples/1.
  , ets:insert(Tid, get_syn_tuples(list))
  , Tuples = lists:sort(ydb_ets_utils:dump_tuples(Tid))
  , ?assertEqual(get_tuples(list), Tuples)

    % Test dump_tuples/2.
  , {T1, T2} = {{ydb_tuple, 4, {6}}, {ydb_tuple, 7, {12}}}
  , {S1, S2} = {{{blah, 4}, T1}, {{blah, 7}, T2}}
  , ets:insert(Tid, [S1, S2])

  , Tuples2 = lists:sort(ydb_ets_utils:dump_tuples(Tid, test))
  , ?assertEqual(get_tuples(list), Tuples2)

  , Tuples3 = lists:sort(ydb_ets_utils:dump_tuples(Tid, blah))
  , ?assertEqual([T1, T2], Tuples3)
.

%%% =============================================================== %%%
%%%  helper functions                                               %%%
%%% =============================================================== %%%

%% @doc Returns all the rows in the table as a list of tuples.
raw_output(Tid) ->
    lists:sort(ets:match_object(Tid, '_'))
.

%% @doc Returns a list of tuples to use.
get_tuples(list) ->
    [
        #ydb_tuple{timestamp=1, data={a, b}}
      , #ydb_tuple{timestamp=2, data={c, d}}
      , #ydb_tuple{timestamp=3, data={e, f}}
      , #ydb_tuple{timestamp=4, data={g, h}}
      , #ydb_tuple{timestamp=5, data={i, j}}
      , #ydb_tuple{timestamp=6, data={k, l}}
    ]
;
get_tuples(tuple) ->
    list_to_tuple(get_tuples(list))
.

%% @doc Converts ydb_tuples to ydb_diff_tuples.
get_diff_tuples(Diff, Tuple=#ydb_tuple{timestamp=TS}) ->
    {Diff, {test, TS}, Tuple}
;

get_diff_tuples(Diff, Tuples) ->
    lists:map(fun(X) -> get_diff_tuples(Diff, X) end, Tuples)
.

%% @doc Checks if all the tuples are ydb_rel_tuples.
is_rel_tuple(Tuples) when is_list(Tuples)->
    lists:foldl(fun(X, Bool) -> Bool and is_rel_tuple(X) end, true, Tuples)
;

is_rel_tuple({{row_num, X}, _Tuple=#ydb_tuple{}}) when is_integer(X)->
    true
;

is_rel_tuple(_) ->
    false
.

%% @doc Converts ydb_tuples to ydb_syn_tuples.
get_syn_tuples(tuple) ->
    list_to_tuple(get_syn_tuples(list))
;

get_syn_tuples(list) ->
    Tuples = get_tuples(list)
  , lists:map(fun(X=#ydb_tuple{timestamp=TS}) -> {{test, TS}, X} end, Tuples)
;

get_syn_tuples(Tuple=#ydb_tuple{timestamp=TS}) ->
    {{test, TS}, Tuple}
.
