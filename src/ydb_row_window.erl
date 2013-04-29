%% @author Max Hirschhorn <maxh@caltech.edu>

%% @doc Module for creating a sliding window of a stream, by number of
%%      rows. Creates a diff of what tuples were added and removed.
-module(ydb_row_window).
-behaviour(ydb_plan_node).

-export([start_link/2, start_link/3]).
-export([init/1, delegate/2, delegate/3, compute_schema/2]).

-record(row_window, {
    size={1, rows} :: {pos_integer(), rows}
  , pulse={1, rows} :: {pos_integer(), rows}

  , diffs=[] :: [ets:tid()]
}).

-type row_window() :: #row_window{
    size :: {pos_integer(), rows}
  , pulse :: {pos_integer(), rows}

  , diffs :: [ets:tid()]
}.
%% Internal state of row window node.

-type option() ::
    {size, {Size :: pos_integer(), rows}}
  | {pulse, {Pulse :: pos_integer(), rows}}
.
%% TODO

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

%% @doc Starts the window node in the supervisor hierarchy.
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

%% @doc Starts the window node in the supervisor hierarchy with a
%%      registered name.
start_link(Name, Args, Options) ->
    ydb_plan_node:start_link(Name, ?MODULE, Args, Options)
.

%% ----------------------------------------------------------------- %%

-spec init(Args :: [option()]) ->
    {ok, State :: row_window()}
  | {error, {badarg, Term :: term()}}
.

%% @doc Initializes the internal state of the window node.
init(Args) when is_list(Args) -> init(Args, #row_window{}).

-spec delegate(Request :: term(), State :: row_window()) ->
    {ok, State :: row_window()}
.

%% @doc TODO
delegate(
    _Request = {tuples, _Tuples}
  , State = #row_window{}
) ->
    {ok, State}
;

delegate(_Request = {info, Message}, State = #row_window{}) ->
    delegate(Message, State)
;

delegate(_Request, State) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec delegate(
    Request :: term()
  , State :: row_window()
  , Extras :: list()
) ->
    {ok, NewState :: row_window()}
.

%% @doc Does nothing.
delegate(_Request, State, _Extras) ->
    {ok, State}
.

-spec compute_schema(
    InputSchemas :: [ydb_plan_node:ydb_schema()]
  , State :: row_window()
) ->
    {ok, OutputSchema :: ydb_plan_node:ydb_schema()}
  | {error, {badarg, InputSchemas :: [ydb_plan_node:ydb_schema()]}}
.

%% @doc Returns the output schema of the row window based upon the
%%      supplied input schemas. Expects a single schema.
compute_schema([Schema], #row_window{}) ->
    {ok, Schema}
;

compute_schema(Schemas, #row_window{}) ->
    {error, {badarg, Schemas}}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: row_window()) ->
    {ok, NewState :: row_window()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Initializes internal state of the window node.
init([], State = #row_window{}) ->
    {ok, State}
;

init([Term | _Args], #row_window{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%
