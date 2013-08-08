%% Header file for a generic publisher process.

-export_type([ydb_gen_pub/0]).

%% @headerfile "ydb_schema.hrl"
-include("ydb_schema.hrl").

-record(ydb_gen_pub, {
    type :: atom()
  , schema=[] :: ydb_schema()

  , callback :: term()
  , subscribers=sets:new() :: set()

  , extras :: term()
}).

%% ----------------------------------------------------------------- %%

-type ydb_gen_pub() :: #ydb_gen_pub{
    type :: atom()
  , schema :: ydb_schema()

  , callback :: term()
  , subscribers :: set()

  , extras :: term()
}.
%% Internal publisher state.

%% ----------------------------------------------------------------- %%
