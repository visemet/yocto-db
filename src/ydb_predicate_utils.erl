%% @author Angela Gong <anjoola@anjoola.com>

%% @private
%% @doc This module contains utility functions used for predicate
%%      comparisons.
-module(ydb_predicate_utils).

-export([is_satisfied/3]).

-include_lib("ydb_plan_node.hrl").

%% ----------------------------------------------------------------- %%

-spec is_satisfied(
    Tuple :: ydb_tuple()
  , Schema :: ydb_schema()
  , Predicate :: clause()) ->
    true | false
.

is_satisfied(_Tuple, _Schema, _Predicate) -> true.



%% ----------------------------------------------------------------- %%
