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
    
### Min

    {ydb_min, child_node, column :: atom() | {atom(), atom()}}
    
### Max

    {ydb_max, child_node, column :: atom() | {atom(), atom()}}
    
### Sum

    {ydb_sum, child_node, column :: atom() | {atom(), atom()}}
    
### Count

    {ydb_count, child_node, column :: atom() | {atom(), atom()}}
    
### Average

    {ydb_avg, child_node, column :: atom() | {atom(), atom()}}
    
### Variance

    {ydb_var, child_node, column :: atom() | {atom(), atom()}}
    
### Standard Deviation

    [ydb_stddev, child_node, column :: atom() | {atom(), atom()}}

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

Table Formats
-------------

Rows in ETS tables are represented as tuples. All the rows in a
given ETS table should be in the same format, which will be one
of the following:

### Relation Table

    ydb_rel_tuple() :: {
        {'row_num', RowNum :: non_neg_integer()}
      , Tuple :: ydb_tuple()
    }.

RowNum serves as a unique id for each row.

### Synopsis Table

    ydb_syn_tuple() :: {
        {Op :: atom(), Timestamp :: non_neg_integer()}
      , Tuple :: ydb_tuple()
    }.

Op is the name of the aggregate (e.g. 'sum' or 'count').

### Diff Table

    ydb_diff_tuple() :: {
        {Diff :: diff(), Op :: atom(), Timestamp :: non_neg_integer()}
      , Tuple :: ydb_tuple()
    }.

Op is the name of the aggregate, as above (e.g. 'sum' or count').
Diff indicates whether the tuple is to be inserted or deleted.

    diff() :: '+' | '-'.
