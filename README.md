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

    
Documentation
-------------

    rebar doc
    
    
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

IO Definitions
--------------

### File Input

    {ydb_file_input,
        child_node
      , filename :: string()
      , batch_size :: integer()
      , poke_freq :: integer()
    }
    
### File Output

    {ydb_file_output, child_node, filename :: string()}
    
### Socket Input

    {ydb_socket_input,
        child_node
      , port_no :: integer()
      , socket :: port()
      , acceptor :: pid()
    }
    
### Socket Output

    {ydb_socket_output,
        child_node
      , port_no :: integer()
      , socket :: port()
      , address :: term()
    }
    

Query Definitions
-----------------

Register a timer that pokes the intermediary plan nodes at each tick,
and store the reference in the supervisor state. This keeps the plan
nodes up to date on the current time so they how to store partial
results.

### Select

    {ydb_select, child_node, predicate :: ydb_clause()}

### Project

The column specification is either just the column name atom() or
a tuple containing the current column name and the new name for the
column {atom(), atom()}.

    {ydb_project, child_node, columns :: [atom() | {atom(), atom()}]}
    
### Joins
   
    {ydb_join_node, left_node, right_node, predicate :: ydb_clause()}
    
### Set Union

    {ydb_union_node, left_node, right_node}
    
### Set Difference
 
    {ydb_diff_node, left_node, right_node}

Predicate Format
----------------

### Boolean Operators

    {ydb_and, clauses :: [ydb_clause()]}
    {ydb_or, clauses :: [ydb_clause()]}
    {ydb_not, clause :: ydb_clause()}

    ydb_clause() :: #ydb_cv{} | #ydb_cc{} | #ydb_and{} | #ydb_or{} | #ydb_not{}

### Comparison Operators

Comparing a column to a value.

    {ydb_cv,
        column :: atom()
      , operator :: compare()
      , value :: term()
    }
    
Comparing two columns together.

    {ydb_cc,
        left_col :: atom()
      , operator :: compare()
      , right_col :: atom()
    }

Comparison operators.

    compare() :: 'gt'  | 'lt'  | 'eq'
               | 'lte' | 'gte' | 'ne'
