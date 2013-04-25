%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc Module for the COUNT aggregate function. Tracks the number of
%%      non-null values seen so far.
-module(ydb_count).
-behaviour(ydb_plan_node).

-export([start_link/2, start_link/3]).
-export([init/1, delegate/2, delegate/3, compute_schema/2]).

%% @headerfile "ydb_plan_node.hrl"
-include("ydb_plan_node.hrl").

% Testing for private functions.
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%% =============================================================== %%%
%%%  internal records and types                                     %%%
%%% =============================================================== %%%

-record(aggr_count, {
    column :: atom() | {atom(), atom()}
  , index :: integer()
  , curr_count=0 :: integer()
}).

-type aggr_count() :: #aggr_count{
    column :: undefined | atom() | {atom(), atom()}
  , index :: undefined | integer()
  , curr_count :: integer()}.
%% Internal count aggregate state.

-type option() ::
    {column, Column :: atom() | {ColName :: atom(), NewName :: atom()}}.
%% Options for the COUNT aggregate:
%% <ul>
%%   <li><code>{column, Column}</code> - The column name to track the
%%       count of. <code>Column</code> is either an atom
%%       <code>Column</code> which is the name of the column, or the
%%       tuple <code>{ColName, NewName}</code> which is the current
%%       name of the column and the desired new name.</li>
%% </ul>

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-spec start_link(Args :: [option()], Options :: list()) ->
    {ok, Pid :: pid()}
  | ignore
  | {error, Error :: term()}
.

%% @doc Starts the input node in the supervisor hierarchy.
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
    {ok, State :: aggr_count()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Initializes the aggregate node's internal state.
init(Args) when is_list(Args) -> init(Args, #aggr_count{});

init(_Args) -> {error, {badarg, not_options_list}}.

-spec delegate(Request :: atom(), State :: aggr_count()) ->
    {ok, State :: aggr_count()}
.

%% @private
%% @doc Passes the new count down to its subscribers.
delegate(
    _Request = {tuple, Tuple}
  , State = #aggr_count{index=Index, curr_count=CurrCount}
) ->
    NewCount = check_tuple(Tuple, Index, CurrCount)
  , NewState = State#aggr_count{curr_count=NewCount}
  , {ok, NewState}
;

delegate(
    _Request = {tuples, Tuples}
  , State = #aggr_count{curr_count=CurrCount, index=Index}
) ->
    NewCount = lists:foldl(
        fun(Tuple, Count) ->
            check_tuple(Tuple, Index, Count)
        end
      , CurrCount
      , Tuples
    )
  , NewState = State#aggr_count{curr_count=NewCount}
  , {ok, NewState}
;

%% @doc Receives the valid index and sets it as part of the state.
delegate(_Request = {index, Index}, State = #aggr_count{}) ->
    NewState = State#aggr_count{index=Index}
  , {ok, NewState}
;

delegate(_Request = {info, Message}, State) ->
    delegate(Message, State)
;

delegate(_Request, State) ->
    {ok, State}
.

-spec delegate(
    Request :: atom()
  , State :: aggr_count()
  , Extras :: list()
) ->
    {ok, NewState :: aggr_count()}
.

delegate(_Request, State, _Extras) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec compute_schema(
    InputSchemas :: [ydb_plan_node:ydb_schema()]
  , State :: aggr_count()
) ->
    {ok, OutputSchema :: ydb_plan_node:ydb_schema()}
  | {error, {badarg, InputSchemas :: [ydb_plan_node:ydb_schema()]}}
.

%% @doc Returns the output schema of the count aggregate based upon
%%      the supplied input schemas. Expects a single schema.
compute_schema([Schema], #aggr_count{column=Column}) ->
    {Index, NewSchema} =
        ydb_aggr_utils:compute_new_schema(Schema, Column, "COUNT")
    % Inform self of index to check for.
  , ydb_plan_node:relegate(
        erlang:self()
      , {index, Index}
    )
  , {ok, NewSchema}
;

compute_schema(Schemas, #aggr_count{}) ->
    {error, {badarg, Schemas}}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: aggr_count()) ->
    {ok, State :: aggr_count()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Parses initializing arguments to set up the internal state of
%%      the count aggregate node.
init([], State = #aggr_count{}) ->
    {ok, State}
;

init([{column, Column} | Args], State = #aggr_count{}) ->
    init(Args, State#aggr_count{column=Column})
;

init([Term | _Args], #aggr_count{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

-spec check_tuple(
    Tuple :: ydb_plan_node:ydb_tuple()
  , Index :: integer()
  , CurrCount :: integer()
) -> NewCount :: integer().

%% @private
%% @doc Selects only the necessary column required for tracking the
%%      count and updates the current count if necessary.
check_tuple(
    Tuple=#ydb_tuple{data=Data}
  , Index
  , CurrCount
) ->
    RelevantData = element(Index, Data)
  , NewCount = get_count(CurrCount, RelevantData)
  , NewTuple = Tuple#ydb_tuple{data=list_to_tuple([NewCount])}
  , ydb_plan_node:notify(
        erlang:self()
      , {tuple, NewTuple}
    )
  , NewCount
.

-spec get_count(Sum :: number(), NewNum :: number()) ->
    NewCount :: number().

%% @doc Increments count if a non-null value is passed in.
get_count(Count, null) ->
    Count
;
get_count(Count, _) ->
    Count + 1
.

%%% =============================================================== %%%
%%%  private tests                                                  %%%
%%% =============================================================== %%%

-ifdef(TEST).
init_test() ->
    ?assertMatch(
        {ok, #aggr_count{column=[first]}}
      , init([], #aggr_count{column=[first]})
    )
  , ?assertMatch(
        {error, {badarg, bad}}
      , init([bad], #aggr_count{})
    )
.
-endif.

%% ----------------------------------------------------------------- %%
