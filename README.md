yocto-db
========

A data-stream management system.

Build & Run
-----------

    rebar clean compile
    erl -pa ebin


Test
----

    rebar eunit

Query Definitions
-----------------

Register a timer that pokes the intermediary plan nodes at each tick,
and store the reference in the supervisor state. This keeps the plan
nodes up to date on the current time so they how to store partial
results.

### Select

    {ydb_select_node, child_node, predicate :: clause()}

### Project

    {ydb_project_node, child_node, specification}

Predicate Format
----------------

### Boolean Operators

    #ydb_and{clauses :: [clause()]}
    #ydb_or{clauses :: [clause()]}
    #ydb_not{clause :: clause()}

    clause() :: #ydb_cv{} | #ydb_cc{} | #ydb_and{} | #ydb_or{} | #ydb_not{}

### Comparison Operators

    #ydb_cv{
        column :: atom()
      , operator :: compare()
      , value :: term()
    }

    #ydb_cc{
        left_column :: atom()
      , operator :: compare()
      , right_column :: atom()
    }

    compare() :: 'gt'  | 'lt'  | 'eq'
               | 'lte' | 'gte' | 'ne'
