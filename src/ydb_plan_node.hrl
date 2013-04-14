%% @author Max Hirschhorn <maxh@caltech.edu>

%% @doc Header file for the plan node. Contains definitions for the
%%      schema, predicates, and operators.

-export_type([ydb_schema/0, ydb_timestamp/0]).

%% A tuple containing the data.
-record(ydb_tuple, {
    timestamp=0 :: non_neg_integer()
  , data={} :: tuple()
}).

%% ----------------------------------------------------------------- %%

-type ydb_schema() :: [
    {Name :: atom(), {Index :: pos_integer(), Type :: atom()}}
].

-type ydb_timestamp() :: '$auto_timestamp' | {atom(), atom()}.

%% ----------------------------------------------------------------- %%

-record(ydb_and, {
    clauses=[] :: [clause()]
}).

-record(ydb_or, {
    clauses=[] :: [clause()]
}).

-record(ydb_not, {
    clause :: clause()
}).

-record(ydb_cv, {
    column :: atom()
  , operator :: compare()
  , value :: term()
}).

-record(ydb_cc, {
    left_column :: atom()
  , operator :: compare()
  , right_column :: atom()
}).

-type clause() :: #ydb_cv{} | #ydb_cc{} | #ydb_and{} | #ydb_or{}
                | #ydb_not{}
.

-type compare() :: 'gt' | 'lt' | 'eq' | 'lte' | 'gte' | 'ne'.

%% ----------------------------------------------------------------- %%
