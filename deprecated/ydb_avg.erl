%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc Module for the AVG aggregate function. Tracks the average
%%      of the values seen so far.
-module(ydb_avg).
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

-record(aggr_avg, {
    column :: atom() | {atom(), atom()}
  , index :: integer()
  , curr_sum :: integer()
  , curr_count=0 :: integer()
}).

-type aggr_avg() :: #aggr_avg{
    column :: undefined | atom() | {atom(), atom()}
  , index :: undefined | integer()
  , curr_sum :: undefined | integer()
  , curr_count :: integer()
}.
%% Internal avg aggregate state.

-type option() ::
    {column, Column :: atom() | {ColName :: atom(), NewName :: atom()}}.
%% Options for the AVG aggregate:
%% <ul>
%%   <li><code>{column, Column}</code> - The column name to track the
%%       average of. <code>Column</code> is either an atom
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
    {ok, State :: aggr_avg()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Initializes the aggregate node's internal state.
init(Args) when is_list(Args) -> init(Args, #aggr_avg{});

init(_Args) -> {error, {badarg, not_options_list}}.

-spec delegate(Request :: atom(), State :: aggr_avg()) ->
    {ok, State :: aggr_avg()}
.

%% @private
%% @doc Passes the new average down to its subscribers.
delegate(
    _Request = {tuple, Tuple}
  , State = #aggr_avg{curr_sum=CurrSum, index=Index, curr_count=CurrCount}
) ->
    {NewSum, NewCount} = check_tuple(Tuple, Index, CurrSum, CurrCount)
  , NewState = State#aggr_avg{curr_sum=NewSum, curr_count=NewCount}
  , {ok, NewState}
;

delegate(
    _Request = {tuples, Tuples}
  , State = #aggr_avg{curr_sum=CurrSum, index=Index, curr_count=CurrCount}
) ->
    {NewSum, NewCount} = lists:foldl(
        fun(Tuple, {Sum, Count}) ->
            check_tuple(Tuple, Index, Sum, Count)
        end
      , {CurrSum, CurrCount}
      , Tuples
    )
  , NewState = State#aggr_avg{curr_sum=NewSum, curr_count=NewCount}
  , {ok, NewState}
;

%% @doc Receives the valid index and sets it as part of the state.
delegate(_Request = {index, Index}, State = #aggr_avg{}) ->
    NewState = State#aggr_avg{index=Index}
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
  , State :: aggr_avg()
  , Extras :: list()
) ->
    {ok, NewState :: aggr_avg()}
.

delegate(_Request, State, _Extras) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec compute_schema(
    InputSchemas :: [ydb_plan_node:ydb_schema()]
  , State :: aggr_avg()
) ->
    {ok, OutputSchema :: ydb_plan_node:ydb_schema()}
  | {error, {badarg, InputSchemas :: [ydb_plan_node:ydb_schema()]}}
.

%% @doc Returns the output schema of the avg aggregate based upon the
%%      supplied input schemas. Expects a single schema.
compute_schema([Schema], #aggr_avg{column=Column}) ->
    {Index, NewSchema} =
        ydb_aggr_utils:compute_new_schema(Schema, Column, "AVG")
    % Inform self of index to check for.
  , ydb_plan_node:relegate(
        erlang:self()
      , {index, Index}
    )
  , {ok, NewSchema}
;

compute_schema(Schemas, #aggr_avg{}) ->
    {error, {badarg, Schemas}}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: aggr_avg()) ->
    {ok, State :: aggr_avg()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Parses initializing arguments to set up the internal state of
%%      the avg aggregate node.
init([], State = #aggr_avg{}) ->
    {ok, State}
;

init([{column, Column} | Args], State = #aggr_avg{}) ->
    init(Args, State#aggr_avg{column=Column})
;

init([Term | _Args], #aggr_avg{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

-spec check_tuple(
    Tuple :: ydb_plan_node:ydb_tuple()
  , Index :: integer()
  , CurrSum :: number()
  , CurrCount :: integer()
) -> {NewSum :: number(), NewCount :: integer()}.

%% @private
%% @doc Selects only the necessary column required for finding the
%%      average, calculates the new average, and updates the current
%%      sum and count values.
check_tuple(
    Tuple=#ydb_tuple{data=Data}
  , Index
  , CurrSum
  , CurrCount
) ->
    RelevantData = element(Index, Data)
  , NewSum = get_sum(CurrSum, RelevantData)
  , NewTuple = Tuple#ydb_tuple{data=list_to_tuple([NewSum/(CurrCount+1)])}
  , ydb_plan_node:notify(
        erlang:self()
      , {tuple, NewTuple}
    )
  , {NewSum, CurrCount + 1}
.

-spec get_sum(Sum :: number(), NewNum :: number()) ->
    NewMin :: number().

%% @doc Gets the sum of two numbers.
get_sum(undefined, NewNum) ->
    NewNum
;

get_sum(Sum, NewNum) when is_number(NewNum) ->
    NewSum = Sum + NewNum
  , NewSum
.

%%% =============================================================== %%%
%%%  private tests                                                  %%%
%%% =============================================================== %%%

-ifdef(TEST).
init_test() ->
    ?assertMatch(
        {ok, #aggr_avg{column=[first]}}
      , init([], #aggr_avg{column=[first]})
    )
  , ?assertMatch(
        {error, {badarg, bad}}
      , init([bad], #aggr_avg{})
    )
.
-endif.

%% ----------------------------------------------------------------- %%
