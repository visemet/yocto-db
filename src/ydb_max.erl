%% @author Angela Gong <anjoola@anjoola.com>

%% @doc Module for the MAX aggregate function. Finds the maximum value
%%      so far.
-module(ydb_max).
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

-record(aggr_max, {
    column :: atom() | {atom(), atom()}
  , index :: integer()
  , curr_max :: integer()
}).

-type aggr_max() :: #aggr_max{
    column :: undefined | atom() | {atom(), atom()}
  , index :: undefined | integer()
  , curr_max :: undefined | integer()}.
%% Internal max aggregate state.

-type option() ::
    {column, Column :: atom() | {ColName :: atom(), NewName :: atom()}}.
%% Options for the MAX aggregate:
%% <ul>
%%   <li><code>{column, Column}</code> - The column name to find the
%%       maximum of. <code>Column</code> is either an atom
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

%% @doc Starts the input node in the supervisor hierarchy with a
%%      registered name.
start_link(Name, Args, Options) ->
    ydb_plan_node:start_link(Name, ?MODULE, Args, Options)
.

%% ----------------------------------------------------------------- %%

-spec init(Args :: [option()]) ->
    {ok, State :: aggr_max()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Initializes the input node's internal state.
init(Args) when is_list(Args) -> init(Args, #aggr_max{});

init(_Args) -> {error, {badarg, not_options_list}}.

-spec delegate(Request :: atom(), State :: aggr_max()) ->
    {ok, State :: aggr_max()}
.

%% @private
%% @doc Passes the new maximum down to its subscribers.
delegate(
    _Request = {tuple, Tuple}
  , State = #aggr_max{curr_max=CurrMax, index=Index}
) ->
    NewMax = check_tuple(Tuple, Index, CurrMax)
  , NewState = State#aggr_max{curr_max=NewMax}
  , {ok, NewState}
;

delegate(
    _Request = {tuples, Tuples}
  , State = #aggr_max{curr_max=CurrMax, index=Index}
) ->
    NewMax = lists:foldl(
        fun(Tuple, Max) ->
            check_tuple(Tuple, Index, Max)
        end
      , CurrMax
      , Tuples
    )
  , NewState = State#aggr_max{curr_max=NewMax}
  , {ok, NewState}
;

%% @doc Receives the new set of valid indexes and sets it as part
%%      of the state.
delegate(_Request = {index, Index}, State = #aggr_max{}) ->
    NewState = State#aggr_max{index=Index}
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
  , State :: aggr_max()
  , Extras :: list()
) ->
    {ok, NewState :: aggr_max()}
.

delegate(_Request, State, _Extras) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec compute_schema(
    InputSchemas :: [ydb_plan_node:ydb_schema()]
  , State :: aggr_max()
) ->
    {ok, OutputSchema :: ydb_plan_node:ydb_schema()}
  | {error, {badarg, InputSchemas :: [ydb_plan_node:ydb_schema()]}}
.

%% @doc Returns the output schema of the max aggregate based upon the
%%      supplied input schemas. Expects a single schema.
compute_schema([Schema], #aggr_max{column=Column}) ->
    {Index, NewSchema} =
        ydb_aggr_utils:compute_new_schema(Schema, Column, "MAX")
    % Inform self of index to check for.
  , ydb_plan_node:relegate(
        erlang:self()
      , {index, Index}
    )
  , {ok, NewSchema}
;   

compute_schema(Schemas, #aggr_max{}) ->
    {error, {badarg, Schemas}}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: aggr_max()) ->
    {ok, State :: aggr_max()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Parses initializing arguments to set up the internal state of
%%      the max aggregate node.
init([], State = #aggr_max{}) ->
    {ok, State}
;

init([{column, Column} | Args], State = #aggr_max{}) ->
    init(Args, State#aggr_max{column=Column})
;

init([Term | _Args], #aggr_max{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

-spec check_tuple(
    Tuple :: ydb_plan_node:ydb_tuple()
  , Index :: integer()
  , CurrMax :: number()
) -> NewMax :: integer().

%% @private
%% @doc Selects only the necessary column required for finding the max
%%      and checks to see if it is indeed the maximum.
check_tuple(
    Tuple=#ydb_tuple{data=Data}
  , Index
  , CurrMax
) ->
    RelevantData = element(Index, Data)
  , NewMax = check_max(CurrMax, RelevantData)
  , NewTuple = Tuple#ydb_tuple{data=list_to_tuple([NewMax])}
  , ydb_plan_node:notify(
        erlang:self()
      , {tuple, NewTuple}
    )
  , NewMax
.

-spec check_max(Max :: number(), NewNum :: number()) ->
    NewMax :: number().

%% @doc Checks two numbers and returns the maximum of them.
check_max(undefined, NewNum) ->
    NewNum
;

check_max(Max, NewNum) when is_integer(NewNum) ->
    NewMax = max(Max, NewNum)
  , NewMax
;

check_max(Max, NewNum) when is_float(NewNum) ->
    NewMax = max(Max, NewNum)
  , NewMax
.

%%% =============================================================== %%%
%%%  private tests                                                  %%%
%%% =============================================================== %%%

-ifdef(TEST).
init_test() ->
    ?assertMatch(
        {ok, #aggr_max{column=[first]}}
      , init([], #aggr_max{column=[first]})
    )
  , ?assertMatch(
        {error, {badarg, bad}}
      , init([bad], #aggr_max{})
    )
.
-endif.

%% ----------------------------------------------------------------- %%
