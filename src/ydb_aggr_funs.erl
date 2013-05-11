%% @author Angela Gong, Kalpana Suraesh
%%         <anjoola@anjoola.com, ksuraesh@caltech.edu>

%% @doc This module contains functions for input into the aggregate
%%      node.
-module(ydb_aggr_funs).

-export([identity/1]).
-export([count_single/2, count_all/1, sum_single/2, sum_all/1,
         avg_single/2, avg_all/1]).
-export([min_single/2, min_all/1, max_single/2, max_all/1]).
-export([var_single/2, var_all/1, stddev_single/2, stddev_all/1]).

%% @headerfile "ydb_plan_node.hrl"
-include("ydb_plan_node.hrl").

%% ----------------------------------------------------------------- %%

-spec identity([term()]) -> term().

%% @doc The identity function. Simply outputs the input.
identity([A]) -> A.

%% ----------------------------------------------------------------- %%

-spec count_single(List :: [term()], Previous :: [integer()]) -> integer().

%% @doc Computes the count over a single diff.
count_single(List, []) ->
    erlang:length(List)
;

count_single(List, [Prev]) ->
    Prev + erlang:length(List)
.

-spec count_all(List :: [term()]) -> integer().

%% @doc Computes the count over all the diffs.
count_all(List) ->
    lists:last(List)
.

%% ----------------------------------------------------------------- %%

-spec sum_single(List :: [term()], Previous :: [term()]) -> term().

%% @doc Computes the sum over a single diff.
sum_single(List, []) ->
    lists:sum(List)
;

sum_single(List, [Prev]) ->
    Prev + lists:sum(List)
.

-spec sum_all(List :: [term()]) -> term().

%% @doc Computes the sum over all the diffs.
sum_all(List) ->
    lists:last(List)
.

%% ----------------------------------------------------------------- %%

-spec avg_single(List :: [term()], Previous :: [integer()]) -> term().

%% @doc Computes the average over a single diff. This is the
%%      PartialFun.
avg_single(List, []) ->
    {lists:length(List), lists:sum(List)}
;
avg_single(List, [{PrevCount, PrevSum}]) ->
    {lists:length(List) + PrevCount, lists:sum(List) + PrevSum}
.

-spec avg_all(List :: [term()]) -> term().

%% @doc Computes the avg over all the diffs. This is the AggrFun.
avg_all(List) ->
    {Count, Sum} = lists:last(List) % get the NewPartial
  , Sum / Count % compute the average
.


%% ----------------------------------------------------------------- %%

-spec min_single(List :: [term()], Previous :: [term()]) -> term().

%% @doc Computes the minimum over a single diff.
min_single(List, []) ->
    lists:min(List)
;

min_single(List, [Prev]) ->
    min(lists:min(List), Prev)
.

-spec min_all(List :: [term()]) -> term().

%% @doc Computes the minimum over all the diffs.
min_all(List) ->
    lists:min(List)
.

%% ----------------------------------------------------------------- %%

-spec max_single(List :: [term()], Previous :: [term()]) -> term().

%% @doc Computes the maximum over a single diff.
max_single(List, []) ->
    lists:max(List)
;

max_single(List, [Prev]) ->
    max(lists:max(List), Prev)
.

-spec max_all(List :: [term()]) -> term().

%% @doc Computes the maximum over all the diffs.
max_all(List) ->
    lists:max(List)
.

%% ----------------------------------------------------------------- %%

-spec var_single(List :: [term()], Previous :: [integer()]) -> term().

%% @doc Computes the variance over a single diff. This is the
%%      PartialFun.
var_single(List, []) ->
    {
        lists:length(List)
      , lists:sum(List)
      , lists:sum(lists:map(fun(X) -> X*X end, List))
    }
;
var_single(List, [{PrevCount, PrevSum, PrevSumSq}]) ->
    {
        lists:length(List) + PrevCount
      , lists:sum(List) + PrevSum
      , lists:sum(lists:map(fun(X) -> X*X end, List)) + PrevSumSq
    }
.

-spec var_all(List :: [term()]) -> term().

%% @doc Computes the avg over all the diffs. This is the AggrFun.
var_all(List) ->
    {Count, Sum, SumSq} = lists:last(List) % get the NewPartial
  , (Count * SumSq - Sum * Sum)/(Count * Count) % compute var
.

%% ----------------------------------------------------------------- %%

-spec stddev_single(List :: [term()], Previous :: [integer()]) -> term().

%% @doc Computes the stddev over a single diff. This is the
%%      PartialFun.
stddev_single(List, []) ->
    {
        lists:length(List)
      , lists:sum(List)
      , lists:sum(lists:map(fun(X) -> X*X end, List))
    }
;
stddev_single(List, [{PrevCount, PrevSum, PrevSumSq}]) ->
    {
        lists:length(List) + PrevCount
      , lists:sum(List) + PrevSum
      , lists:sum(lists:map(fun(X) -> X*X end, List)) + PrevSumSq
    }
.

-spec stddev_all(List :: [term()]) -> term().

%% @doc Computes the stddev over all the diffs. This is the AggrFun.
stddev_all(List) ->
    {Count, Sum, SumSq} = lists:last(List)
  , math:sqrt((Count * SumSq - Sum * Sum)/(Count * Count))
.

%% ----------------------------------------------------------------- %%
