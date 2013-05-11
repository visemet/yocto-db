%% @author Angela Gong <anjoola@anjoola.com>

%% @doc This module contains functions for input into the aggregate
%%      node.
-module(ydb_aggr_funs).

-export([identity/1]).
-export([count_single/2, count_all/1, sum_single/2, sum_all/1]).
-export([min_single/2, min_all/1, max_single/2, max_all/1]).

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

%% @doc Computes the average over a single diff.
avg_single(List, []) ->
    lists:sum(List) / lists:length(List)
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

%% ----------------------------------------------------------------- %%

%% ----------------------------------------------------------------- %%
