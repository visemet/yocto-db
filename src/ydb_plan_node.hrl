%% @author Max Hirschhorn <maxh@caltech.edu>

%% @doc Header file for the plan node.

-export_type([ydb_schema/0]).

%% @type ydb_tuple() = #ydb_tuple{
%%           timestamp :: non_neg_integer()
%%         , data :: tuple()
%%       }
%%       .
-record(ydb_tuple, {
    timestamp=0 :: non_neg_integer()
  , data={} :: tuple()
}).

%% ----------------------------------------------------------------- %%

-type ydb_schema() :: [
    {Name :: atom(), {Index :: pos_integer(), Type :: atom()}}
].

%% ----------------------------------------------------------------- %%
