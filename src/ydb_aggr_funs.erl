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
-export([count_priv_single/2, count_priv_all/1]).

%% @headerfile "ydb_plan_node.hrl"
-include("ydb_plan_node.hrl").

%% ----------------------------------------------------------------- %%

-spec identity([term()]) -> term().

%% @doc The identity function. Simply outputs the input.
identity([A]) -> A.

%% ----------------------------------------------------------------- %%

-spec count_single(List :: [term()], Previous :: [integer()]) -> integer().

%% @doc Computes the count over a single diff. This is the PartialFun.
count_single(List, []) ->
    erlang:length(List)
;

count_single(List, [Prev]) ->
    Prev + erlang:length(List)
.

-spec count_all(List :: [term()]) -> integer().

%% @doc Computes the count over all the diffs. This is the AggrFun.
count_all(List) ->
    lists:last(List)
.

%% ----------------------------------------------------------------- %%

-spec sum_single(List :: [term()], Previous :: [term()]) -> term().

%% @doc Computes the sum over a single diff. This is the PartialFun.
sum_single(List, []) ->
    lists:sum(List)
;

sum_single(List, [Prev]) ->
    Prev + lists:sum(List)
.

-spec sum_all(List :: [term()]) -> term().

%% @doc Computes the sum over all the diffs. This is the AggrFun.
sum_all(List) ->
    lists:last(List)
.

%% ----------------------------------------------------------------- %%

-spec avg_single(List :: [term()], Previous :: [integer()]) -> term().

%% @doc Computes the average over a single diff. This is the
%%      PartialFun.
avg_single(List, []) ->
    {erlang:length(List), lists:sum(List)}
;
avg_single(List, [{PrevCount, PrevSum}]) ->
    {erlang:length(List) + PrevCount, lists:sum(List) + PrevSum}
.

-spec avg_all(List :: [term()]) -> term().

%% @doc Computes the avg over all the diffs. This is the AggrFun.
avg_all(List) ->
    {Count, Sum} = lists:last(List)
  , Sum / Count
.


%% ----------------------------------------------------------------- %%

-spec min_single(List :: [term()], Previous :: [term()]) -> term().

%% @doc Computes the minimum over a single diff. This is the
%%      PartialFun.
min_single(List, []) ->
    lists:min(List)
;

min_single(List, [Prev]) ->
    min(lists:min(List), Prev)
.

-spec min_all(List :: [term()]) -> term().

%% @doc Computes the minimum over all the diffs. This is the AggrFun.
min_all(List) ->
    lists:min(List)
.

%% ----------------------------------------------------------------- %%

-spec max_single(List :: [term()], Previous :: [term()]) -> term().

%% @doc Computes the maximum over a single diff. This is the
%%      PartialFun.
max_single(List, []) ->
    lists:max(List)
;

max_single(List, [Prev]) ->
    max(lists:max(List), Prev)
.

-spec max_all(List :: [term()]) -> term().

%% @doc Computes the maximum over all the diffs. This is the AggrFun.
max_all(List) ->
    lists:max(List)
.

%% ----------------------------------------------------------------- %%

-spec var_single(List :: [term()], Previous :: [integer()]) -> term().

%% @doc Computes the population variance over a single diff. This is
%%      the PartialFun.
var_single(List, []) ->
    {
        erlang:length(List)
      , lists:sum(List)
      , lists:sum(lists:map(fun(X) -> X * X end, List))
    }
;
var_single(List, [{PrevCount, PrevSum, PrevSumSq}]) ->
    {
        erlang:length(List) + PrevCount
      , lists:sum(List) + PrevSum
      , lists:sum(lists:map(fun(X) -> X * X end, List)) + PrevSumSq
    }
.

-spec var_all(List :: [term()]) -> term().

%% @doc Computes the population variance over all the diffs. This is
%%      the AggrFun.
var_all(List) ->
    {Count, Sum, SumSq} = lists:last(List)
  , (Count * SumSq - Sum * Sum) / (Count * Count)
.

%% ----------------------------------------------------------------- %%

-spec stddev_single(List :: [term()], Previous :: [integer()]) -> term().

%% @doc Computes the population standard deviation over a single diff.
%%      This is the PartialFun.
stddev_single(List, []) ->
    {
        erlang:length(List)
      , lists:sum(List)
      , lists:sum(lists:map(fun(X) -> X * X end, List))
    }
;
stddev_single(List, [{PrevCount, PrevSum, PrevSumSq}]) ->
    {
        erlang:length(List) + PrevCount
      , lists:sum(List) + PrevSum
      , lists:sum(lists:map(fun(X) -> X * X end, List)) + PrevSumSq
    }
.

-spec stddev_all(List :: [term()]) -> term().

%% @doc Computes the population standard deviation over all the diffs.
%%      This is the AggrFun.
stddev_all(List) ->
    {Count, Sum, SumSq} = lists:last(List)
  , math:sqrt((Count * SumSq - Sum * Sum) / (Count * Count))
.

%% ----------------------------------------------------------------- %%

-spec count_priv_single(
    List :: [{term(), integer(), {number(), atom()}}]
  , Previous :: [{
        integer()
      , number()
      , number()
      , {number()} | {number(), dict()}
      , integer()
    }]
) ->
    {
        NewTime :: integer()
      , NewL :: number()
      , NewLT :: number()
      , NewM :: {number()} | {number(), dict()}
      , Init :: integer()
    }.

%% @doc Computes the count over a single diff. This is the
%%      PartialFun. Expects List to be a list of tuples of the form
%%      {Data, Timestamp, Options={Epsilon, Mechanism}}
count_priv_single(List = [{_Data, Timestamp, _Opts} | _Rest], []) ->
    lists:foldl(
        fun(Tuple, Partial) -> update_private_state(Tuple, Partial) end
      , {0, 0, 0, undefined, Timestamp - 1}
      , List
    )
;
count_priv_single(List, [Prev={_Time, _L, _LT, _M, _Init}]) ->
    lists:foldl(
        fun(Tuple, Partial) -> update_private_state(Tuple, Partial) end
      , Prev
      , List
    )
.

-spec count_priv_all(List :: [term()]) -> term().

%% @doc Computes the count over all the diffs. This is the AggrFun.
count_priv_all(List) ->
    {_Time, _L, LT, M, _Init} = lists:last(List) % get the NewPartial
  , round(LT + element(1, M))
.

%% ----------------------------------------------------------------- %%

-spec update_private_state(
    New :: {term(), integer(), {number(), atom()}}
  , Partial :: {
        integer()
      , number()
      , number()
      , {number()} | {number(), dict()}
      , integer()
    }
) ->
    {
        NewTime :: integer()
      , NewL :: number()
      , NewLT :: number()
      , NewM :: {number()} | {number(), dict()}
      , Init :: integer()
    }.
%% @doc Takes in the current partial results for a privacy-preserving
%%      aggregates and updates the results for an incoming tuple.
update_private_state(
    _New={_Data, Timestamp, _Options={Eps, Mech}}
  , _Partial={Time, L, LT, M, Init}
) ->
    NewTime = Timestamp - Init
  , Sigma = 1
  , {NewL, NewLT} = ydb_private_utils:do_logarithmic_advance(
        {L, LT}, Time, NewTime, Sigma, Eps/2)
  , NewM = ydb_private_utils:do_bounded_advance(
        M, Time, NewTime, Sigma, Eps/2, Mech)
  , {NewTime, NewL, NewLT, NewM, Init}
.