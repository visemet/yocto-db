%% Header file for a generic spout process.

-export_type([ydb_spout_id/0]).

-define(Spout(Id), {spout, Id}).

%% ----------------------------------------------------------------- %%

-type ydb_spout_id() :: {'spout', Id :: atom()}.
%% Identifier for spout process.

%% ----------------------------------------------------------------- %%
