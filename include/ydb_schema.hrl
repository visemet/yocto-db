%% Header file for the schema of a plan node or input stream.

-export_type([ydb_schema/0]).

%% ----------------------------------------------------------------- %%

-type ydb_schema() :: [{
    Name :: atom()
  , {Index :: pos_integer(), Type :: atom()}
}].
%% Defines the schema of a plan node or input stream. Represented as a
%% mapping (list) of a column name to a pair containing an index and a
%% type.

%% ----------------------------------------------------------------- %%
