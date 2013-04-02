-export_type([ydb_schema/0]).

-record(ydb_tuple, {
    timestamp=0 :: non_neg_integer()
  , data={} :: tuple()
}).

%% ----------------------------------------------------------------- %%

-type ydb_schema() :: [
    {Name :: atom(), {Index :: pos_integer(), Type :: atom()}}
].

%% ----------------------------------------------------------------- %%
