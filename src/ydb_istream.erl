%% @author Max Hirschhorn <maxh@caltech.edu>

%% @doc Module for converting a diff into a stream. Extracts the
%%      inserted tuples.
-module(ydb_istream).
-behaviour(ydb_plan_node).

-export([start_link/2, start_link/3]).
-export([init/1, delegate/2, delegate/3, compute_schema/2]).

-record(istream, {}).

-type istream() :: #istream{}.
%% Internal state of the istream node.

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

%% @doc Starts the istream node in the supervisor hierarchy.
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

%% @doc Starts the istream node in the supervisor hierarchy with a
%%      registered name.
start_link(Name, Args, Options) ->
    ydb_plan_node:start_link(Name, ?MODULE, Args, Options)
.

%% ----------------------------------------------------------------- %%

-spec init(Args :: [option()]) ->
    {ok, State :: istream()}
  | {error, {badarg, Term :: term()}}
.

%% @doc Initializes the internal state of the istream node.
init(Args) when is_list(Args) -> init(Args, #istream{}).

-spec delegate(Request :: term(), State :: istream()) ->
    {ok, State :: istream()}
.

%% @doc Extracts the inserted tuples from the diffs.
delegate({diffs, Diffs}, State = #istream{}) when is_list(Diffs) ->
    lists:foreach(
        fun (Diff) ->
            Tuples = get_inserts([Diff])
          , ydb_plan_node:send_tuples(erlang:self(), Tuples)
        end

      , Diffs
    )

  , {ok, State}
;

delegate({info, Message}, State = #istream{}) ->
    delegate(Message, State)
;

delegate(_Request, State) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec delegate(
    Request :: term()
  , State :: istream()
  , Extras :: list()
) ->
    {ok, NewState :: istream()}
.

%% @doc Does nothing.
delegate(_Request, State, _Extras) ->
    {ok, State}
.

-spec compute_schema(
    InputSchemas :: [ydb_plan_node:ydb_schema()]
  , State :: istream()
) ->
    {ok, OutputSchema :: ydb_plan_node:ydb_schema()}
  | {error, {badarg, InputSchemas :: [ydb_plan_node:ydb_schema()]}}
.

%% @doc Returns the output schema of the istream node based upon the
%%      supplied input schemas. Expects a single schema.
compute_schema([Schema], #istream{}) ->
    {ok, Schema}
;

compute_schema(Schemas, #istream{}) ->
    {error, {badarg, Schemas}}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: istream()) ->
    {ok, NewState :: istream()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Initializes the internal state of the istream node.
init([], State = #istream{}) ->
    {ok, State}
;

init([Term | _Args], #istream{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

-spec get_inserts(Diffs :: [ets:tid()]) ->
    Tuples :: [ydb_plan_node:ydb_tuple()]
.

%% @doc Returns the list of PLUS (`+') tuples from the diffs.
get_inserts(Diffs) when is_list(Diffs) ->
    {Plus, _Minus} = ydb_ets_utils:extract_diffs(Diffs)
  , Plus
.

%% ----------------------------------------------------------------- %%
