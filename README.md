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

Supervisor Hierarchy
--------------------

    +ydb_sup
    |
    +---+ydb_input_stream_sup
        |
        +---+ydb_input_stream
        |   |
        |   +---+ydb_{file,socket}_input
        |       |
        |       +ydb_branch_node
        |
        +ydb_query_sup
        |
        +---+ydb_query

The ydb\_input\_stream\_sup module supports registering input streams
either from a file or a socket. The input stream process also spawns
a branch node process and adds it as a subscriber.

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
    
### Joins
   
    {ydb_join_node, left_node, right_node, predicate :: clause()}
    
### Set Union

    {ydb_union_node, left_node, right_node}
    
### Set Difference
 
    {ydb_diff_node, left_node, right_node}

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
