%% Header file for a generic bolt process.

-export_type([ydb_bolt_id/0]).

-define(Bolt(Id), {bolt, Id}).

%% ----------------------------------------------------------------- %%

-type ydb_bolt_id() :: {'bolt', Id :: atom()}.
%% Identifier for bolt process.

%% ----------------------------------------------------------------- %%
