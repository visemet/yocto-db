%% @author Max Hirschhorn <maxh@caltech.edu>

%% @doc TODO
-module(ydb_aggr_node).
-behaviour(ydb_plan_node).

-export([start_link/2, start_link/3]).
-export([init/1, delegate/2, delegate/3, compute_schema/2]).

-record(aggr, {
    history_size=1 :: pos_integer() | 'infinity'
  , grouped=false :: boolean()

  , aggr_fun :: 'undefined' | fun()
  , synopsis :: 'undefined' | ets:tid()
}).

-type aggr() :: #aggr{
    history_size :: pos_integer()
  , grouped :: boolean()

  , aggr_fun :: 'undefined' | fun()
  , synopsis :: 'undefined' | ets:tid()
}.
%% Internal state of aggregate node.

-type option() ::
    {history_size, HistorySize :: pos_integer() | 'infinity'}
  | {grouped, Grouped :: boolean()}
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

%% @doc Starts the aggregate node in the supervisor hierarchy.
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

%% @doc Starts the aggregate node in the supervisor hierarchy with a
%%      registered name.
start_link(Name, Args, Options) ->
    ydb_plan_node:start_link(Name, ?MODULE, Args, Options)
.

%% ----------------------------------------------------------------- %%

-spec init(Args :: [option()]) ->
    {ok, State :: aggr()}
  | {error, {badarg, Term :: term()}}
.

%% @doc Initializes the internal state of the aggregate node.
init(Args) when is_list(Args) -> init(Args, #aggr{}).

-spec delegate(Request :: term(), State :: aggr()) ->
    {ok, NewState :: aggr()}
.

%% @doc TODO
delegate(
    _Request = {tuples, _Tuples}
  , State = #aggr{}
) ->
    {ok, State}
;

delegate(
    _Request = {diffs, _Diffs}
  , State = #aggr{}
) ->
    {ok, State}
;

delegate(_Request = {info, Message}, State = #aggr{}) ->
    delegate(Message, State)
;

delegate(_Request, State) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec delegate(
    Request :: term()
  , State :: aggr()
  , Extras :: list()
) ->
    {ok, NewState :: aggr()}
.

%% @doc Does nothing.
delegate(_Request, State, _Extras) ->
    {ok, State}
.

-spec compute_schema(
    InputSchemas :: [ydb_plan_node:ydb_schema()]
  , State :: aggr()
) ->
    {ok, OutputSchema :: ydb_plan_node:ydb_schema()}
  | {error, {badarg, InputSchemas :: [ydb_plan_node:ydb_schema()]}}
.

%% @doc Returns the output schema of the aggregate node based upon the
%%      supplied input schemas. Expects a single schema.
compute_schema([Schema], #aggr{}) ->
    {ok, Schema}
;

compute_schema(Schemas, #aggr{}) ->
    {error, {badarg, Schemas}}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

%% @private
%% @doc Initializes internal state of the aggregate node.
init([], State = #aggr{}) ->
    {ok, State}
;

init([Term | _Args], #aggr{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%
