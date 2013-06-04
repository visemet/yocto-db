%% @author Angela Gong, Kalpana Suraesh
%%         <anjoola@anjoola.com, ksuraesh@caltech.edu>

%% @doc This module contains functions for input into the aggregate
%%      node.
-module(ydb_aggr_funs).

-export([get_aggr/1, identity/1, make_private/4]).

%% @headerfile "ydb_plan_node.hrl"
-include("ydb_plan_node.hrl").

%% Dict that uses {Aggr, Incremental, Private} as the key and the
%% corresponding funs in the tuple {PrFun, AggrFun} as the value.
-define(FUN_TABLE, dict:from_list([
    % non-incremental, non-private aggregates
    {{sum, false, false},
        {(fun (A) -> sum_single(A, []) end), (fun sum_all_noninc/1)}}
  , {{count, false, false},
        {(fun (A) -> count_single(A, []) end), (fun count_all_noninc/1)}}
  , {{avg, false, false},
        {(fun (A) -> avg_single(A, []) end), (fun avg_all_noninc/1)}}
  , {{min, false, false},
        {(fun (A) -> min_single(A, []) end), (fun min_all_noninc/1)}}
  , {{max, false, false},
        {(fun (A) -> max_single(A, []) end), (fun max_all_noninc/1)}}
  , {{stddev, false, false},
        {(fun (A) -> stddev_single(A, []) end), (fun stddev_all_noninc/1)}}
  , {{var, false, false},
        {(fun (A) -> var_single(A, []) end), (fun var_all_noninc/1)}}

    % incremental, non-private aggregates
  , {{sum, true, false},
        {(fun sum_single/2), (fun sum_all_inc/1)}}
  , {{count, true, false},
        {(fun count_single/2), (fun count_all_inc/1)}}
  , {{avg, true, false},
        {(fun avg_single/2), (fun avg_all_inc/1)}}
  , {{min, true, false},
        {(fun min_single/2), (fun min_all_inc/1)}}
  , {{max, true, false},
        {(fun max_single/2), (fun max_all_inc/1)}}
  , {{stddev, true, false},
        {(fun stddev_single/2), (fun stddev_all_inc/1)}}
  , {{var, true, false},
        {(fun var_single/2), (fun var_all_inc/1)}}

    % non-incremental, private aggregates (windowed aggregates)
  , {{sum, false, true},
        {(fun sum_priv_single/1), (fun sum_priv_all_noninc/1)}}
  , {{count, false, true},
        {(fun count_priv_single/1), (fun count_priv_all_noninc/1)}}
  , {{avg, false, true},
        {(fun avg_priv_single/1), (fun avg_priv_all_noninc/1)}}

    % incremental, private aggregates (infinite history/non-windowed aggrs)
  , {{sum, true, true},
        {(fun sum_priv_single/2), (fun sum_priv_all/1)}}
  , {{count, true, true},
        {(fun count_priv_single/2), (fun count_priv_all/1)}}
  , {{avg, true, true},
        {(fun avg_priv_single/2), (fun avg_priv_all/1)}}

])).


-record(aggr, {
    name :: atom()
  , incremental=false :: boolean()
  , private=false :: boolean()
}).
%% Internal state.

-type option() ::
    {name, OperatorName :: atom()}
  | {incremental, Incremental :: boolean()}
  | {private, Private :: boolean()}.
%% Options for the aggregate node:
%% <ul>
%%   <li><code>{name, OperatorName}</code> - Name of the aggregate
%%       function desired. Can be any one of <code>{sum, count, avg,
%%       min, max, stddev, var}</code>.</li>
%%   <li><code>{incremental, Incremental}</code> - Whether or not the
%%       aggregate function is to be done incrementally.</li>
%%   <li><code>{private, Private}</code> - Is <code>true</code> if a
%%       privacy-preserving version of the aggregate function is
%%       desired. Currently only implemented for <code>count</code> and
%%       <code>sum</code>.</li>
%% </ul>

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-spec identity([term()]) -> term().

%% @doc The identity function. Simply outputs the input.
identity([A]) -> A.

%% ----------------------------------------------------------------- %%

-spec get_aggr(Args :: [option()]) -> {
    PrFun :: fun(([term()]) -> term()) | fun(([term()], [term()]) -> term())
  , AggrFun :: fun(([term()]) -> term())
}.

%% @doc Entry point into <code>ydb_aggr_funs</code>. Returns the funs
%%      necessary to do the desired aggregate function.
get_aggr(Options) ->
    find_aggr(Options, #aggr{})
.

-spec find_aggr(Args :: [option()], State :: #aggr{}) -> {
    PrFun :: fun(([term()]) -> term()) | fun(([term()], [term()]) -> term())
  , AggrFun :: fun(([term()]) -> term())
}.

%% @private
%% @doc Helper function for get_aggr.
find_aggr([{private, Private} | Args], State = #aggr{}) ->
    find_aggr(Args, State#aggr{private=Private})
;

find_aggr([{incremental, Incremental} | Args], State = #aggr{}) ->
    find_aggr(Args, State#aggr{incremental=Incremental})
;

find_aggr([{name, Name} | Args], State = #aggr{}) ->
    find_aggr(Args, State#aggr{name=Name})
;

find_aggr([], _State = #aggr{
    name = Name
  , incremental = Incremental
  , private = Private
}) ->
    dict:fetch({Name, Incremental, Private}, ?FUN_TABLE)
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec count_single(List :: [term()], Previous :: [integer()]) -> integer().

%% @doc Computes the count over a single diff. This is the PartialFun
%%      for the non-incremental version.
count_single(List, []) ->
    erlang:length(List)
;

% Incremental version.
count_single(List, Prev) ->
    PrevCount = lists:last(Prev)
  , PrevCount + erlang:length(List)
.

-spec count_all_noninc(List :: [term()]) -> integer().

%% @doc Computes the count over all the diffs. This is the AggrFun for
%%      the non-incremental version.
count_all_noninc(List) ->
    lists:sum(List)
.

-spec count_all_inc(List :: [term()]) -> integer().

%% @doc Computes the count over all the diffs. This is the AggrFun for
%%      the incremental version.
count_all_inc(List) ->
    lists:last(List)
.

%% ----------------------------------------------------------------- %%

-spec sum_single(List :: [term()], Previous :: [term()]) -> term().

%% @doc Computes the sum over a single diff. This is the PartialFun
%%      for the non-incremental version.
sum_single(List, []) ->
    lists:sum(List)
;

% Incremental version.
sum_single(List, Prev) ->
    PrevSum = lists:last(Prev)
  , PrevSum + lists:sum(List)
.

-spec sum_all_noninc(List :: [term()]) -> integer().

%% @doc Computes the sum over all the diffs. This is the AggrFun for
%%      the non-incremental version.
sum_all_noninc(List) ->
    lists:sum(List)
.

-spec sum_all_inc(List :: [term()]) -> term().

%% @doc Computes the sum over all the diffs. This is the AggrFun for
%%      the incremental version.
sum_all_inc(List) ->
    lists:last(List)
.

%% ----------------------------------------------------------------- %%

-spec avg_single(List :: [term()], Previous :: [term()]) -> term().

%% @doc Computes the average over a single diff. This is the
%%      PartialFun for the non-incremental version.
avg_single(List, []) ->
    {erlang:length(List), lists:sum(List)}
;

% Incremental version.
avg_single(List, Prev) ->
    {PrevCount, PrevSum} = lists:last(Prev)
  , {erlang:length(List) + PrevCount, lists:sum(List) + PrevSum}
.

-spec avg_all_noninc(List :: [number()]) -> float().

%% @doc Computes the average over all the diffs. This is the AggrFun
%%      for the non-incremental version.
avg_all_noninc(List) ->
    {TotalCount, TotalSum} = lists:foldl(
        fun(X, {Count, Sum}) ->
            {Count + element(1, X), Sum + element(2, X)}
        end
      , {0, 0}
      , List
    )
  , TotalSum / TotalCount
.

-spec avg_all_inc(List :: [number()]) -> float().

%% @doc Computes the average over all the diffs. This is the AggrFun
%%      for the incremental version.
avg_all_inc(List) ->
    {Count, Sum} = lists:last(List)
  , Sum / Count
.

%% ----------------------------------------------------------------- %%

-spec min_single(List :: [term()], Previous :: [term()]) -> term().

%% @doc Computes the minimum over a single diff. This is the PartialFun
%%      for the non-incremental version.
min_single(List, []) ->
    lists:min(List)
;

% Incremental version.
min_single(List, Prev) ->
    min(lists:min(List), lists:min(Prev))
.

-spec min_all_noninc(List :: [term()]) -> term().

%% @doc Computes the minimum over all the diffs. This is the AggrFun
%%      for the non-incremental version.
min_all_noninc(List) ->
    lists:min(List)
.

-spec min_all_inc(List :: [term()]) -> term().

%% @doc Computes the minimum over all the diffs. This is the AggrFun
%%      for the incremental version.
min_all_inc(List) ->
    lists:last(List)
.

%% ----------------------------------------------------------------- %%

-spec max_single(List :: [term()], Previous :: [term()]) -> term().

%% @doc Computes the maximum over a single diff. This is the
%%      PartialFun for the non-incremental version.
max_single(List, []) ->
    lists:max(List)
;

% Incremental version.
max_single(List, Prev) ->
    max(lists:max(List), lists:max(Prev))
.

-spec max_all_noninc(List :: [term()]) -> term().

%% @doc Computes the maximum over all the diffs. This is the AggrFun
%%      for the non-incremental version.
max_all_noninc(List) ->
    lists:max(List)
.

-spec max_all_inc(List :: [term()]) -> term().

%% @doc Computes the maximum over all the diffs. This is the AggrFun
%%      for the incremental version.
max_all_inc(List) ->
    lists:last(List)
.

%% ----------------------------------------------------------------- %%

-spec var_single(List :: [term()], Previous :: [integer()]) -> term().

%% @doc Computes the population variance over a single diff. This is
%%      the PartialFun for the non-incremental version.
var_single(List, []) ->
    {
        erlang:length(List)
      , lists:sum(List)
      , lists:sum(lists:map(fun(X) -> X * X end, List))
    }
;

% Incremental version.
var_single(List, Prev) ->
    {PrevCount, PrevSum, PrevSumSq} = lists:last(Prev)
  , {
        erlang:length(List) + PrevCount
      , lists:sum(List) + PrevSum
      , lists:sum(lists:map(fun(X) -> X * X end, List)) + PrevSumSq
    }
.

-spec var_all_noninc(List :: [term()]) -> term().

%% @doc Computes the population variance over all the diffs. This is
%%      the AggrFun for the non-incremental version.
var_all_noninc(List) ->
    {TotalCount, TotalSum, TotalSumSq} = lists:foldl(
        fun(X, {Count, Sum, SumSq}) ->
            {
                Count + element(1, X)
              , Sum + element(2, X)
              , SumSq + element(3, X)
            }
        end
      , {0, 0, 0}
      , List
    )
  , (TotalCount * TotalSumSq - TotalSum * TotalSum) / (TotalCount * TotalCount)
.

-spec var_all_inc(List :: [term()]) -> term().

%% @doc Computes the population variance over all the diffs. This is
%%      the AggrFun for the incremental version.
var_all_inc(List) ->
    {Count, Sum, SumSq} = lists:last(List)
  , (Count * SumSq - Sum * Sum) / (Count * Count)
.

%% ----------------------------------------------------------------- %%

-spec stddev_single(List :: [term()], Previous :: [integer()]) -> term().

%% @doc Computes the population standard deviation over a single diff.
%%      This is the PartialFun for the non-incremental version.
stddev_single(List, []) ->
    {
        erlang:length(List)
      , lists:sum(List)
      , lists:sum(lists:map(fun(X) -> X * X end, List))
    }
;

% Incremental version.
stddev_single(List, Prev) ->
    {PrevCount, PrevSum, PrevSumSq} = lists:last(Prev)
  , {
        erlang:length(List) + PrevCount
      , lists:sum(List) + PrevSum
      , lists:sum(lists:map(fun(X) -> X * X end, List)) + PrevSumSq
    }
.

-spec stddev_all_noninc(List :: [term()]) -> term().

%% @doc Computes the population standard deviation over all the diffs.
%%      This is the AggrFun for the non-incremental version.
stddev_all_noninc(List) ->
    {TotalCount, TotalSum, TotalSumSq} = lists:foldl(
        fun(X, {Count, Sum, SumSq}) ->
            {
                Count + element(1, X)
              , Sum + element(2, X)
              , SumSq + element(3, X)
            }
        end
      , {0, 0, 0}
      , List
    )
  , math:sqrt(
        (TotalCount * TotalSumSq - TotalSum * TotalSum)
      / (TotalCount * TotalCount)
    )
.

-spec stddev_all_inc(List :: [term()]) -> term().

%% @doc Computes the population standard deviation over all the diffs.
%%      This is the AggrFun for the incremental version.
stddev_all_inc(List) ->
    {Count, Sum, SumSq} = lists:last(List)
  , math:sqrt((Count * SumSq - Sum * Sum) / (Count * Count))
.

%% ----------------------------------------------------------------- %%

-spec make_private(
    EvalFun :: fun(([term()]) -> term())
  , Epsilon :: number()
  , Mechanism :: atom()
  , MinInterval :: integer()
) -> fun(([term()]) -> term()).

%% @doc Takes in an existing eval_fun and returns the output required
%%      to evaluate that output in a privacy-preserving manner. This
%%      is the EvalFun for privacy-preserving aggregates. For an
%%      eval_fun that produces a value Data, this will return a fun
%%      that produces a tuple of the form:
%%          {Data, Timestamp, Options={Epsilon, Mechanism, MinInterval}}.
%%      The list of terms passed to the resulting fun to be evaluated
%%      are expected to be prefixed with the timestamp.
make_private(EvalFun, Epsilon, Mechanism, MinInterval) ->
    fun([TS | Cols]) ->
        {EvalFun(Cols), TS, {Epsilon, Mechanism, MinInterval}}
    end
.

%% ----------------------------------------------------------------- %%

-spec count_priv_single(
    List :: [{term(), integer(), {number(), atom(), integer()}}]
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
%%      {Data, Timestamp, Options={Epsilon, Mechanism, MinInterval}}
% count_priv_single(List, []) -> List; % windowed only

count_priv_single(List, []) ->
    {_Data, Timestamp, {_Eps, _Mech, MinInterval}} = lists:nth(1, List)
  , lists:foldl(
        fun(Tuple, Partial) -> update_private_state(Tuple, Partial, count) end
      , {0, 0, 0, undefined, Timestamp - MinInterval}
      , List
    )
;

count_priv_single(List, PrevList) ->
    Prev = lists:last(PrevList)
  , lists:foldl(
        fun(Tuple, Partial) -> update_private_state(Tuple, Partial, count) end
      , Prev
      , List
    )
.

-spec count_priv_single(
    List :: [{term(), integer(), {number(), atom(), integer()}}]
) ->
    List :: [term()].

%% @doc Produes the partial result for a single diff. This is the
%%      PartialFun. Expects List to be a list of tuples of the form
%%      {Data, Timestamp, Options={Epsilon, Mechanism, MinInterval}}
%%      Nonincremental.
count_priv_single(List) ->
    List
.

-spec count_priv_all_noninc(List :: [term()]) -> term().

%% @doc Computes the sum over all the diffs. This is the AggrFun.
count_priv_all_noninc(List) ->
    FlatList = lists:append(List)
  , {_Data, Timestamp, {_Eps, _Mech, MinInterval}} = erlang:hd(FlatList)

    % Find the size of the widnow
  , MaxT = lists:foldl(
        fun({_D, T, _O}, Mx) -> max(T, Mx) end, Timestamp, FlatList)
  , MinT = lists:foldl(
        fun({_D, T, _O}, Mn) -> min(T, Mn) end, Timestamp, FlatList)
  , W = MaxT - MinT + 1 % window size over these diffs

  , ResultPartial = lists:foldl(
        fun(Tuple, Partial) ->
            update_private_state_bounded(Tuple, Partial, W, 'count') end
      , {0, undefined, Timestamp - MinInterval}
      , FlatList
    )

  , {_Time, M, _Init} = ResultPartial
  , round(element(1, M))
.


-spec count_priv_all(List :: [term()]) -> term().

%% @doc Computes the count over all the diffs. This is the AggrFun.
count_priv_all(List) ->
    {_Time, _L, LT, M, _Init} = lists:last(List)
  , round(LT + element(1, M))
.

%% ----------------------------------------------------------------- %%

-spec sum_priv_single(
    List :: [{term(), integer(), {number(), atom(), integer()}}]
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

%% @doc Computes the sum over a single diff. This is the
%%      PartialFun. Expects List to be a list of tuples of the form
%%      {Data, Timestamp, Options={Epsilon, Mechanism, MinInterval}}
% sum_priv_single(List, []) -> List; % windowed only

sum_priv_single(List, []) ->
    {_Data, Timestamp, {_Eps, _Mech, MinInterval}} = lists:last(List)
  , lists:foldl(
        fun(Tuple, Partial) -> update_private_state(Tuple, Partial, sum) end
      , {0, 0, 0, undefined, Timestamp - MinInterval}
      , List
    )
;

sum_priv_single(List, PrevList) ->
    Prev = lists:last(PrevList)
  , lists:foldl(
        fun(Tuple, Partial) -> update_private_state(Tuple, Partial, sum) end
      , Prev
      , List
    )
.

-spec sum_priv_single(
    List :: [{term(), integer(), {number(), atom(), integer()}}]
) ->
    List :: [term()].

%% @doc Produes the partial result for a single diff. This is the
%%      PartialFun. Expects List to be a list of tuples of the form
%%      {Data, Timestamp, Options={Epsilon, Mechanism, MinInterval}}
%%      Nonincremental.
sum_priv_single(List) ->
    List
.

-spec sum_priv_all_noninc(List :: [term()]) -> term().

%% @doc Computes the sum over all the diffs. This is the AggrFun.
sum_priv_all_noninc(List) ->
    FlatList = lists:append(List)
  , {_Data, Timestamp, {_Eps, _Mech, MinInterval}} = erlang:hd(FlatList)

    % Find the size of the widnow
  , MaxT = lists:foldl(
        fun({_D, T, _O}, Mx) -> max(T, Mx) end, Timestamp, FlatList)
  , MinT = lists:foldl(
        fun({_D, T, _O}, Mn) -> min(T, Mn) end, Timestamp, FlatList)
  , W = MaxT - MinT + 1 % window size over these diffs

  , ResultPartial = lists:foldl(
        fun(Tuple, Partial) ->
            update_private_state_bounded(Tuple, Partial, W, 'sum') end
      , {0, undefined, Timestamp - MinInterval}
      , FlatList
    )

  , {_Time, M, _Init} = ResultPartial
  , round(element(1, M))
.

-spec sum_priv_all(List :: [term()]) -> term(). % incremental

%% @doc Computes the sum over all the diffs. This is the AggrFun.
sum_priv_all(List) ->
    {_Time, _L, LT, M, _Init} = lists:last(List)
  , round(LT + element(1, M))
.

%% ----------------------------------------------------------------- %%

-spec avg_priv_single(
    List :: [{term(), integer(), {number(), atom(), integer()}}]
  , Previous :: [{
        integer()
      , number()
      , number()
      , {number()} | {number(), dict()}
      , integer()
      , integer()
    }]
) ->
    {
        NewTime :: integer()
      , NewL :: number()
      , NewLT :: number()
      , NewM :: {number()} | {number(), dict()}
      , Init :: integer()
      , NewC :: integer()
    }.

%% @doc Computes the average over a single diff. This is the
%%      PartialFun. Expects List to be a list of tuples of the form
%%      {Data, Timestamp, Options={Epsilon, Mechanism, MinInterval}}
%avg_priv_single(List, []) -> List; % windowed only

avg_priv_single(List, []) ->
    {_Data, Timestamp, {_Eps, _Mech, MinInterval}} = lists:last(List)
  , NewSumPartial = lists:foldl(
        fun(Tuple, Partial) -> update_private_state(Tuple, Partial, sum) end
      , {0, 0, 0, undefined, Timestamp - MinInterval}
      , List
    )
  , erlang:append_element(NewSumPartial, erlang:length(List))
;

avg_priv_single(List, PrevList) ->
    {T, L, LT, M, I, C} = lists:last(PrevList)
  , NewSumPartial = lists:foldl(
        fun(Tuple, Partial) -> update_private_state(Tuple, Partial, sum) end
      , {T, L, LT, M, I} % don't care about the count being private
      , List
    )
  , erlang:append_element(NewSumPartial, C + erlang:length(List))
.

-spec avg_priv_single(
    List :: [{term(), integer(), {number(), atom(), integer()}}]
) ->
    List :: [term()].

%% @doc Produes the partial result for a single diff. This is the
%%      PartialFun. Expects List to be a list of tuples of the form
%%      {Data, Timestamp, Options={Epsilon, Mechanism, MinInterval}}
%%      Nonincremental.
avg_priv_single(List) ->
    List
.

-spec avg_priv_all_noninc(List :: [term()]) -> term().

%% @doc Computes the sum over all the diffs. This is the AggrFun.
avg_priv_all_noninc(List) ->
    FlatList = lists:append(List)
  , {_Data, Timestamp, {_Eps, _Mech, MinInterval}} = erlang:hd(FlatList)

    % Find the size of the widnow
  , MaxT = lists:foldl(
        fun({_D, T, _O}, Mx) -> max(T, Mx) end, Timestamp, FlatList)
  , MinT = lists:foldl(
        fun({_D, T, _O}, Mn) -> min(T, Mn) end, Timestamp, FlatList)
  , W = MaxT - MinT + 1 % window size over these diffs

  , ResultPartial = lists:foldl(
        fun(Tuple, Partial) ->
            update_private_state_bounded(Tuple, Partial, W, 'sum') end
      , {0, undefined, Timestamp - MinInterval}
      , FlatList
    )

  , {_Time, M, _Init} = ResultPartial
  , element(1, M) / erlang:length(FlatList)
.

-spec avg_priv_all(List :: [term()]) -> term().

%% @doc Computes the average over all the diffs. This is the AggrFun.
avg_priv_all(List) ->
    {_Time, _L, LT, M, _Init, C} = lists:last(List)
  , round(LT + element(1, M))/C
.


%% ----------------------------------------------------------------- %%

-spec update_private_state_bounded(
    New :: {term(), integer(), {number(), atom(), integer()}}
  , Partial :: {
        integer()
      , {number()} | {number(), dict()}
      , integer()
    }
  , W :: integer()
  , Aggr :: 'sum' | 'count'
) ->
    {
        NewTime :: integer()
      , NewM :: {number()} | {number(), dict()} | 'undefined'
      , Init :: integer()
    }.

%% @doc Takes in the current partial results for a privacy-preserving
%%      aggregates and updates the results for an incoming tuple.
update_private_state_bounded(
    _New={Data, Timestamp, _Options={Eps, _Mech, MinInterval}}
  , _Partial={_Time, M, Init}
  , W
  , Aggr
) ->
    NewTime = trunc((Timestamp - Init) / MinInterval)
  , Sigma = Data
  , NewM = ydb_private_utils:do_bounded_binary_advance(
        M, NewTime, Sigma, Eps/2, W, Aggr)
  , {NewTime, NewM, Init}
.


%% ----------------------------------------------------------------- %%

-spec update_private_state(
    New :: {term(), integer(), {number(), atom(), integer()}}
  , Partial :: {
        integer()
      , number()
      , number()
      , {number()} | {number(), dict()}
      , integer()
    }
  , Aggr :: atom()
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
    _New={_Data, Timestamp, _Options={Eps, Mech, MinInterval}}
  , _Partial={Time, L, LT, M, Init}
  , count
) ->
    NewTime = trunc((Timestamp - Init) / MinInterval)
  , Sigma = 1
  , {NewL, NewLT} = ydb_private_utils:do_logarithmic_advance(
        {L, LT}, Time, NewTime, Sigma, Eps/2)
  , NewM = ydb_private_utils:do_bounded_advance(
        M, Time, NewTime, Sigma, Eps/2, Mech)
  , {NewTime, NewL, NewLT, NewM, Init}
;

update_private_state(
    _New={Data, Timestamp, _Options={Eps, Mech, MinInterval}}
  , _Partial={Time, L, LT, M, Init}
  , sum
) ->
    NewTime = trunc((Timestamp - Init) / MinInterval)
  , Sigma = Data
  , {NewL, NewLT} = ydb_private_utils:do_logarithmic_advance(
        {L, LT}, Time, NewTime, Sigma, Eps/2)
  , NewM = ydb_private_utils:do_bounded_sum_advance(
        M, Time, NewTime, Sigma, Eps/2, Mech)
  , {NewTime, NewL, NewLT, NewM, Init}
.