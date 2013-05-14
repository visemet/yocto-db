%% @author Angela Gong, Kalpana Suraesh
%%         <anjoola@anjoola.com, ksuraesh@caltech.edu>

%% @doc This module contains functions for input into the aggregate
%%      node.
-module(ydb_aggr_funs).

-export([identity/1]).
-export([get_aggr/1]).

%% @headerfile "ydb_plan_node.hrl"
-include("ydb_plan_node.hrl").

%% @doc Incremental aggregate function table for the partial result
%%      funs.
-define(PR_FUN_TABLE, dict:from_list([
    {sum, fun sum_single/2}
  , {count, fun count_single/2}
  , {avg, fun avg_single/2}
  , {min, fun min_single/2}
  , {max, fun max_single/2}
  , {stddev, fun stddev_single/2}
  , {var, fun var_single/2}
])).

%% @doc Non-incremental aggregate function table for the partial
%%      result funs.
-define(PR_FUN_NONINCREMENTAL_TABLE, dict:from_list([
    {sum, {error, invalid_aggregate}}
  , {count, {error, invalid_aggregate}}
  , {avg, {error, invalid_aggregate}}
  , {min, {error, invalid_aggregate}}
  , {max, {error, invalid_aggregate}}
  , {stddev, {error, invalid_aggregate}}
  , {var, {error, invalid_aggregate}}
])).

%% @doc Partial result funs for private aggregates.
-define(PRIVATE_PR_FUN_TABLE, dict:from_list([
    {sum, {error, invalid_aggregate}}
  , {count, fun count_priv_single/2}
  , {avg, {error, invalid_aggregate}}
  , {min, {error, invalid_aggregate}}
  , {max, {error, invalid_aggregate}}
  , {stddev, {error, invalid_aggregate}}
  , {var, {error, invalid_aggregate}}
])).

%% @doc Aggregate function table for the overall aggregate funs.
-define(AGGR_FUN_TABLE, dict:from_list([
    {sum, fun sum_all/1}
  , {count, fun count_all/1}
  , {avg, fun avg_all/1}
  , {min, fun min_all/1}
  , {max, fun max_all/1}
  , {stddev, fun stddev_all/1}
  , {var, fun var_all/1}
])).

%% @doc Overall aggregate funs for private aggregates.
-define(PRIVATE_AGGR_FUN_TABLE, dict:from_list([
    {sum, {error, invalid_aggregate}}
  , {count, fun count_priv_all/1}
  , {avg, {error, invalid_aggregate}}
  , {min, {error, invalid_aggregate}}
  , {max, {error, invalid_aggregate}}
  , {stddev, {error, invalid_aggregate}}
  , {var, {error, invalid_aggregate}}
])).

-record(aggr, {
    name :: atom()
  , incremental :: boolean()
  , private :: boolean()
}).

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
%%       aggregation to be done is incremental. Is <code>true</code> if
%%       so, <code>false</code> otherwise.</li>
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
    find_aggr(Args, State=#aggr{private=Private})
;

find_aggr([{incremental, Incremental} | Args], State = #aggr{}) ->
    find_aggr(Args, State=#aggr{incremental=Incremental})
;

find_aggr([{name, Name} | Args], State = #aggr{}) ->
    find_aggr(Args, State=#aggr{name=Name})
;

find_aggr([], _State = #aggr{
    private = Private
  , incremental = Incremental
  , name = Name
}) ->
    % Private operators.
    if
        % Private operators.
        Private == true -> {
            dict:fetch(Name, ?PRIVATE_PR_FUN_TABLE)
          , dict:fetch(Name, ?PRIVATE_AGGR_FUN_TABLE)
        }
        % Incremental operators.
      ; Incremental -> {
            dict:fetch(Name, ?PR_FUN_TABLE)
          , dict:fetch(Name, ?AGGR_FUN_TABLE)
        }
        % Non-incremental operators.
      ; true ->  {
            dict:fetch(Name, ?PR_FUN_NONINCREMENTAL_TABLE)
          , dict:fetch(Name, ?AGGR_FUN_TABLE)
        }
    end
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

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
