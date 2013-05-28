%% Header file for a tuple produced and consumed by plan nodes.

-export_type([ydb_tuple/0]).

-record(ydb_tuple, {
    timestamp=0 :: non_neg_integer()
  , data={} :: tuple()
}).

%% ----------------------------------------------------------------- %%

-type ydb_tuple() :: #ydb_tuple{
    timestamp :: non_neg_integer()
  , data :: tuple()
}.
%% Defines the representation of a tuple in the data-stream management
%% system. Contains attributes for the timestamp and actual data
%% values.

%% ----------------------------------------------------------------- %%
