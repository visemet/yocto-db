%% Header file for the schema of a plan node.

-export_type([ydb_schema/0]).

%% ----------------------------------------------------------------- %%

-type ydb_schema() :: [{
    Name :: atom()
  , Index :: pos_integer()
}].
%% Defines the schema of a plan node. Represented as a mapping (list of
%% tuples) of a column name to an index position.

%% ----------------------------------------------------------------- %%
