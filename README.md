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

    {ydb_select_node, child_node, predicate}

### Project

    {ydb_project_node, child_node, specification}
