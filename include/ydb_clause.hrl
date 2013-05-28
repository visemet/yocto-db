%% Header file for the clauses and comparison operators used by plan
%% nodes.

-export_type([ydb_compare/0, ydb_clause/0]).

-record(ydb_cv, {
    column :: atom()
  , operator :: ydb_compare()
  , value :: term()
}).

-record(ydb_cc, {
    left_col :: atom()
  , operator :: ydb_compare()
  , right_col :: atom()
}).

-record(ydb_and, {
    clauses=[] :: [ydb_clause()]
}).

-record(ydb_or, {
    clauses=[] :: [ydb_clause()]
}).

-record(ydb_not, {
    clause :: ydb_clause()
}).

%% ----------------------------------------------------------------- %%

-type ydb_compare() :: 'gt' | 'lt' | 'eq' | 'gte' | 'lte' | 'ne'.
%% Comparison operators, which can be one of the following:
%% <ul>
%%   <li><code>'gt'</code> - greater than</li>
%%   <li><code>'lt'</code> - less than</li>
%%   <li><code>'eq'</code> - equal to</li>
%%   <li><code>'gte'</code> - greater than or equal to</li>
%%   <li><code>'lte'</code> - less than or equal to</li>
%%   <li><code>'ne'</code> - not equal to</li>
%% </ul>

-type ydb_and() :: #ydb_and{
    clauses :: [ydb_clause()]}.
%% A list of clauses to be <code>and</code>ed together.

-type ydb_or() :: #ydb_or{
    clauses :: [ydb_clause()]}.
%% A list of clauses to be <code>or</code>ed together.

-type ydb_not() :: #ydb_not{clause :: ydb_clause()}.
%% A clause to be negated.

-type ydb_cv() :: #ydb_cv{
    column :: atom()
  , operator :: compare()
  , value :: term()
}.
%% Compares a named column <code>Column</code> to <code>Value</code>
%% with the comparison operator <code>Operator</code>.

-type ydb_cc() :: #ydb_cc{
    left_col :: atom()
  , operator :: compare()
  , right_col :: atom()
}.
%% Compares a column <code>LeftCol</code> to another column
%% <code>RightCol</code> with the comparison operator
%% <code>Operator</code>.

-type ydb_clause() ::
    ydb_cv()
  | ydb_cc()
  | ydb_and()
  | ydb_or()
  | ydb_not()
.
%% A clause involving operations on columns.

%% ----------------------------------------------------------------- %%
