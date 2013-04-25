%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc Module for the SUM aggregate function. Tracks the sum of the
%%      values seen so far.
-module(ydb_sum).
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

-record(aggr_sum, {
    column :: atom() | {atom(), atom()}
  , index :: integer()
  , curr_sum=0 :: integer()
}).

-type aggr_sum() :: #aggr_sum{
    column :: undefined | atom() | {atom(), atom()}
  , index :: undefined | integer()
  , curr_sum :: integer()}.
%% Internal sum aggregate state.

-type option() ::
    {column, Column :: atom() | {ColName :: atom(), NewName :: atom()}}.
%% Options for the SUM aggregate:
%% <ul>
%%   <li><code>{column, Column}</code> - The column name to track the
%%       sum of. <code>Column</code> is either an atom
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
    {ok, State :: aggr_sum()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Initializes the aggregate node's internal state.
init(Args) when is_list(Args) -> init(Args, #aggr_sum{});

init(_Args) -> {error, {badarg, not_options_list}}.

-spec delegate(Request :: atom(), State :: aggr_sum()) ->
    {ok, State :: aggr_sum()}
.

%% @private
%% @doc Passes the new sum down to its subscribers.
delegate(
    _Request = {tuple, Tuple}
  , State = #aggr_sum{curr_sum=CurrSum, index=Index}
) ->
    NewSum = check_tuple(Tuple, Index, CurrSum)
  , NewState = State#aggr_sum{curr_sum=NewSum}
  , {ok, NewState}
;

delegate(
    _Request = {tuples, Tuples}
  , State = #aggr_sum{curr_sum=CurrSum, index=Index}
) ->
    NewSum = lists:foldl(
        fun(Tuple, Sum) ->
            check_tuple(Tuple, Index, Sum)
        end
      , CurrSum
      , Tuples
    )
  , NewState = State#aggr_sum{curr_sum=NewSum}
  , {ok, NewState}
;

%% @doc Receives the new set of valid indexes and sets it as part
%%      of the state.
delegate(_Request = {index, Index}, State = #aggr_sum{}) ->
    NewState = State#aggr_sum{index=Index}
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
  , State :: aggr_sum()
  , Extras :: list()
) ->
    {ok, NewState :: aggr_sum()}
.

delegate(_Request, State, _Extras) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec compute_schema(
    InputSchemas :: [ydb_plan_node:ydb_schema()]
  , State :: aggr_sum()
) ->
    {ok, OutputSchema :: ydb_plan_node:ydb_schema()}
  | {error, {badarg, InputSchemas :: [ydb_plan_node:ydb_schema()]}}
.

%% @doc Returns the output schema of the sum aggregate based upon the
%%      supplied input schemas. Expects a single schema.
compute_schema([Schema], #aggr_sum{column=Column}) ->
    {Index, NewSchema} =
        ydb_aggr_utils:compute_new_schema(Schema, Column, "SUM")
    % Inform self of index to check for.
  , ydb_plan_node:relegate(
        erlang:self()
      , {index, Index}
    )
  , {ok, NewSchema}
;

compute_schema(Schemas, #aggr_sum{}) ->
    {error, {badarg, Schemas}}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: aggr_sum()) ->
    {ok, State :: aggr_sum()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Parses initializing arguments to set up the internal state of
%%      the sum aggregate node.
init([], State = #aggr_sum{}) ->
    {ok, State}
;

init([{column, Column} | Args], State = #aggr_sum{}) ->
    init(Args, State#aggr_sum{column=Column})
;

init([Term | _Args], #aggr_sum{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

-spec check_tuple(
    Tuple :: ydb_plan_node:ydb_tuple()
  , Index :: integer()
  , CurrMin :: number()
) -> NewMin :: integer().

%% @private
%% @doc Selects only the necessary column required for finding the sum
%%      and adds it to the current sum.
check_tuple(
    Tuple=#ydb_tuple{data=Data}
  , Index
  , CurrSum
) ->
    RelevantData = element(Index, Data)
  , NewSum = get_sum(CurrSum, RelevantData)
  , NewTuple = Tuple#ydb_tuple{data=list_to_tuple([NewSum])}
  , ydb_plan_node:notify(
        erlang:self()
      , {tuple, NewTuple}
    )
  , NewSum
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
        {ok, #aggr_sum{column=[first]}}
      , init([], #aggr_sum{column=[first]})
    )
  , ?assertMatch(
        {error, {badarg, bad}}
      , init([bad], #aggr_sum{})
    )
.
-endif.

%% ----------------------------------------------------------------- %%
