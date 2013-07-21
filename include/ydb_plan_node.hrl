%% Header file for the plan node. Contains definitions for the schema,
%% predicates, and operators.

-export_type([compare/0, ydb_clause/0, ydb_tuple/0]).
-export_type([ydb_schema/0, ydb_timestamp/0]).

-export_type([ydb_plan_op/0, ydb_node_id/0]).

-record(ydb_tuple, {
    timestamp=0 :: non_neg_integer()
  , data={} :: tuple()
}).

-type ydb_tuple() :: #ydb_tuple{
    timestamp :: non_neg_integer()
  , data :: tuple()}.
%% A tuple representation in the database. Contains the timestamp and
%% and the actual values in the <code>data</code> field.

%% ----------------------------------------------------------------- %%

-type ydb_schema() :: [
    {Name :: atom(), {Index :: pos_integer(), Type :: atom()}}].
%% The schema of the data. is a list of columns, where each column is
%% specified by a name <code>Name</code>, and a tuple containing their
%% index <code>Index</code> and type <code>Type</code>.

-type ydb_timestamp() :: '$auto_timestamp'
                       | {Units :: atom(), Time :: atom()}.
%% A timestamp. <code>Units</code> can be one of the following:
%% <ul>
%%   <li><code>micro_sec</code></li> - microseconds
%%   <li><code>milli_sec</code></li> - millisecond
%%   <li><code>sec</code></li> - second
%%   <li><code>min</code></li> - minute
%%   <li><code>hour</code></li> - hour
%% </ul>

%% ----------------------------------------------------------------- %%

-type compare() :: 'gt' | 'lt' | 'eq' | 'gte' | 'lte' | 'ne'.
%% Comparison operators, which can be one of the following:
%% <ul>
%%   <li><code>'gt'</code> - greater than</li>
%%   <li><code>'lt'</code> - less than</li>
%%   <li><code>'eq'</code> - equal to</li>
%%   <li><code>'gte'</code> - greater than or equal to</li>
%%   <li><code>'lte'</code> - less than or equal to</li>
%%   <li><code>'ne'</code> - not equal to</li>
%% </ul>

-record(ydb_and, {
    clauses=[] :: [ydb_clause()]
}).

-type ydb_and() :: #ydb_and{
    clauses :: [ydb_clause()]}.
%% A list of clauses to be <code>and</code>ed together.

-record(ydb_or, {
    clauses=[] :: [ydb_clause()]
}).

-type ydb_or() :: #ydb_or{
    clauses :: [ydb_clause()]}.
%% A list of clauses to be <code>or</code>ed together.

-record(ydb_not, {
    clause :: ydb_clause()
}).

-type ydb_not() :: #ydb_not{clause :: ydb_clause()}.
%% A clause to be negated.

-record(ydb_cv, {
    column :: atom()
  , operator :: compare()
  , value :: term()
}).

-type ydb_cv() :: #ydb_cv{
    column :: atom()
  , operator :: compare()
  , value :: term()
}.
%% Compares a named column <code>Column</code> to <code>Value</code>
%% with the comparison operator <code>Operator</code>.

-record(ydb_cc, {
    left_col :: atom()
  , operator :: compare()
  , right_col :: atom()
}).

-type ydb_cc() :: #ydb_cc{
    left_col :: atom()
  , operator :: compare()
  , right_col :: atom()
}.
%% Compares a column <code>LeftCol</code> to another column
%% <code>RightCol</code> with the comparison operator
%% <code>Operator</code>.

-type ydb_clause() :: #ydb_cv{} | ydb_cc() | #ydb_and{} | #ydb_or{} | #ydb_not{}.
%% A clause involving operations on columns.

%% ----------------------------------------------------------------- %%

-type ydb_plan_op() :: atom().

-type ydb_node_id() :: {Type :: ydb_plan_op(), Id :: pos_integer()}.

%% ----------------------------------------------------------------- %%
