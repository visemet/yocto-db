%% Header file for a generic bolt process.

-export_type([ydb_bolt_op/0, ydb_bolt_id/0]).

%% ----------------------------------------------------------------- %%

-type ydb_bolt_op() :: atom().

-type ydb_bolt_id() :: {Type :: ydb_bolt_op(), Id :: pos_integer()}.

%% ----------------------------------------------------------------- %%
