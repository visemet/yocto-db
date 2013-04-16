%% Header file for the plan node. Contains definitions for the schema,
%% predicates, and operators.

-record(ydb_tuple, {
    timestamp=0 :: non_neg_integer()
  , data={} :: tuple()
}).

-type ydb_tuple() :: {
    Timestamp :: non_neg_integer()
  , Data :: tuple()
}.
%% A tuple containing the data. Has a timestamp of
%% <code>Timestamp</code> and data <code>Data</code>.

%% ----------------------------------------------------------------- %%

-type ydb_schema() :: [
    {Name :: atom(), {Index :: pos_integer(), Type :: atom()}}
].
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
    clauses=[] :: [clause()]
}).

-type ydb_and() :: {[clause()]}.
%% A list of clauses to be <code>and</code>ed together.

-record(ydb_or, {
    clauses=[] :: [clause()]
}).

-type ydb_or() :: {[clause()]}.
%% A list of clauses to be <code>or</code>ed together.

-record(ydb_not, {
    clause :: clause()
}).

-type ydb_not() :: clause().
%% A clause to be negated.

-record(ydb_cv, {
    column :: atom()
  , operator :: compare()
  , value :: term()
}).

-type ydb_cv() :: {
    Column :: atom()
  , Operator :: compare()
  , Value :: term()
}.
%% Compares a named column <code>Column</code> to <code>Value</code>
%% with the comparison operator <code>Operator</code>.

-record(ydb_cc, {
    left_col :: atom()
  , operator :: compare()
  , right_col :: atom()
}).

-type ydb_cc() :: {
    LeftCol :: atom()
  , Operator :: compare()
  , RightCol :: atom()
}.
%% Compares a column <code>LeftCol</code> to another column
%% <code>RightCol</code> with the comparison operator
%% <code>Operator</code>.

-type clause() :: ydb_cv() | ydb_cc() | ydb_and() | ydb_or() | ydb_not().
%% A clause involving operations on columns.

%% ----------------------------------------------------------------- %%
