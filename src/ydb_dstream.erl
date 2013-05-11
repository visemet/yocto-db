%% @author Max Hirschhorn <maxh@caltech.edu>

%% @doc Module for converting a diff into a stream. Extracts the
%%      deleted tuples.
-module(ydb_dstream).
-behaviour(ydb_plan_node).

-export([start_link/2, start_link/3]).
-export([init/1, delegate/2, delegate/3, compute_schema/2]).

-record(dstream, {}).

-type dstream() :: #dstream{}.
%% Internal state of the dstream node.

-type option() :: {}.
%% No options.

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-spec start_link(
    Args :: [option()]
  , Options :: list()
) ->
    {ok, Pid :: pid()}
  | ignore
  | {error, Error :: term()}
.

%% @doc Starts the dstream node in the supervisor hierarchy.
start_link(Args, Options) ->
    ydb_plan_node:start_link(?MODULE, Args, Options)
.

-spec start_link(
    Name :: atom()
  , Args :: [option()]
  , Options :: list()
) ->
    {ok, Pid :: pid()}
  | ignore
  | {error, Error :: term()}
.

%% @doc Starts the dstream node in the supervisor hierarchy with a
%%      registered name.
start_link(Name, Args, Options) ->
    ydb_plan_node:start_link(Name, ?MODULE, Args, Options)
.

%% ----------------------------------------------------------------- %%

-spec init(Args :: [option()]) ->
    {ok, State :: dstream()}
  | {error, {badarg, Term :: term()}}
.

%% @doc Initializes the internal state of the dstream node.
init(Args) when is_list(Args) -> init(Args, #dstream{}).

-spec delegate(Request :: term(), State :: dstream()) ->
    {ok, State :: dstream()}
.

%% @doc Extracts the deleted tuples from the diffs.
delegate({diffs, Diffs}, State = #dstream{}) when is_list(Diffs) ->
    Tuples = get_deletes(Diffs)

  , ydb_plan_node:send_tuples(erlang:self(), Tuples)

  , {ok, State}
;

delegate({info, Message}, State = #dstream{}) ->
    delegate(Message, State)
;

delegate(_Request, State) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec delegate(
    Request :: term()
  , State :: dstream()
  , Extras :: list()
) ->
    {ok, NewState :: dstream()}
.

%% @doc Does nothing.
delegate(_Request, State, _Extras) ->
    {ok, State}
.

-spec compute_schema(
    InputSchemas :: [ydb_plan_node:ydb_schema()]
  , State :: dstream()
) ->
    {ok, OutputSchema :: ydb_plan_node:ydb_schema()}
  | {error, {badarg, InputSchemas :: [ydb_plan_node:ydb_schema()]}}
.

%% @doc Returns the output schema of the dstream node based upon the
%%      supplied input schemas. Expects a single schema.
compute_schema([Schema], #dstream{}) ->
    {ok, Schema}
;

compute_schema(Schemas, #dstream{}) ->
    {error, {badarg, Schemas}}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: dstream()) ->
    {ok, NewState :: dstream()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Initializes the internal state of the dstream node.
init([], State = #dstream{}) ->
    {ok, State}
;

init([Term | _Args], #dstream{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

-spec get_deletes(Diffs :: [ets:tid()]) ->
    Tuples :: [ydb_plan_node:ydb_tuple()]
.

%% @doc Returns the list of MINUS (`-') tuples from the diffs.
get_deletes(Diffs) when is_list(Diffs) ->
    {_Plus, Minus} = ydb_ets_utils:extract_diffs(Diffs)
  , Minus
.

%% ----------------------------------------------------------------- %%
