%% @author Angela Gong <anjoola@anjoola.com>

%% @doc Module for the MIN aggregate function. Finds the minimum value
%%      so far.
-module(ydb_min).
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

-record(aggr_min, {
    column :: atom() | {atom(), atom()}
  , index :: integer()
  , curr_min :: integer()
}).

-type aggr_min() :: #aggr_min{
    column :: undefined | atom() | {atom(), atom()}
  , index :: undefined | integer()
  , curr_min :: undefined | integer()}.
%% Internal min aggregate state.

-type option() ::
    {column, Column :: atom() | {ColName :: atom(), NewName :: atom()}}.
%% Options for the MIN aggregate:
%% <ul>
%%   <li><code>{column, Column}</code> - The column name to find the
%%       minimum of. <code>Column</code> is either an atom
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
    {ok, State :: aggr_min()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Initializes the input node's internal state.
init(Args) when is_list(Args) -> init(Args, #aggr_min{});

init(_Args) -> {error, {badarg, not_options_list}}.

-spec delegate(Request :: atom(), State :: aggr_min()) ->
    {ok, State :: aggr_min()}
.

%% @private
%% @doc Passes the new minimum down to its subscribers.
delegate(
    _Request = {tuple, Tuple}
  , State = #aggr_min{curr_min=CurrMin, index=Index}
) ->
    NewMin = check_tuple(Tuple, Index, CurrMin)
  , NewState = State#aggr_min{curr_min=NewMin}
  , {ok, NewState}
;

delegate(
    _Request = {tuples, Tuples}
  , State = #aggr_min{curr_min=CurrMin, index=Index}
) ->
    NewMin = lists:foldl(
        fun(Tuple, Min) ->
            check_tuple(Tuple, Index, Min)
        end
      , CurrMin
      , Tuples
    )
  , NewState = State#aggr_min{curr_min=NewMin}
  , {ok, NewState}
;

%% @doc Receives the new set of valid indexes and sets it as part
%%      of the state.
delegate(_Request = {index, Index}, State = #aggr_min{}) ->
    NewState = State#aggr_min{index=Index}
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
  , State :: aggr_min()
  , Extras :: list()
) ->
    {ok, NewState :: aggr_min()}
.

delegate(_Request, State, _Extras) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec compute_schema(
    InputSchemas :: [ydb_plan_node:ydb_schema()]
  , State :: aggr_min()
) ->
    {ok, OutputSchema :: ydb_plan_node:ydb_schema()}
  | {error, {badarg, InputSchemas :: [ydb_plan_node:ydb_schema()]}}
.

%% @doc Returns the output schema of the min aggregate based upon the
%%      supplied input schemas. Expects a single schema.
compute_schema([Schema], #aggr_min{column=Column}) ->
    {Index, NewSchema} =
        ydb_aggr_utils:compute_new_schema(Schema, Column, "MIN")
    % Inform self of index to check for.
  , ydb_plan_node:relegate(
        erlang:self()
      , {index, Index}
    )
  , {ok, NewSchema}
;   

compute_schema(Schemas, #aggr_min{}) ->
    {error, {badarg, Schemas}}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: aggr_min()) ->
    {ok, State :: aggr_min()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Parses initializing arguments to set up the internal state of
%%      the min aggregate node.
init([], State = #aggr_min{}) ->
    {ok, State}
;

init([{column, Column} | Args], State = #aggr_min{}) ->
    init(Args, State#aggr_min{column=Column})
;

init([Term | _Args], #aggr_min{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

-spec check_tuple(
    Tuple :: ydb_plan_node:ydb_tuple()
  , Index :: integer()
  , CurrMin :: number()
) -> NewMin :: integer().

%% @private
%% @doc Selects only the necessary column required for finding the min
%%      and checks to see if it is indeed the minimum.
check_tuple(
    Tuple=#ydb_tuple{data=Data}
  , Index
  , CurrMin
) ->
    RelevantData = element(Index, Data)
  , NewMin = check_min(CurrMin, RelevantData)
  , NewTuple = Tuple#ydb_tuple{data=list_to_tuple([NewMin])}
  , ydb_plan_node:notify(
        erlang:self()
      , {tuple, NewTuple}
    )
  , ydb_plan_node:notify(
        erlang:self()
      , {new_min, NewMin}
    )
  , NewMin
.

-spec check_min(Min :: number(), NewNum :: number()) ->
    NewMin :: number().

%% @doc Checks two numbers and returns the minimum of them.
check_min(undefined, NewNum) ->
    NewNum
;

check_min(Min, NewNum) when is_integer(NewNum) ->
    NewMin = min(Min, NewNum)
  , NewMin
;

check_min(Min, NewNum) when is_float(NewNum) ->
    NewMin = min(Min, NewNum)
  , NewMin
.

%%% =============================================================== %%%
%%%  private tests                                                  %%%
%%% =============================================================== %%%

-ifdef(TEST).
init_test() ->
    ?assertMatch(
        {ok, #aggr_min{column=[first]}}
      , init([], #aggr_min{column=[first]})
    )
  , ?assertMatch(
        {error, {badarg, bad}}
      , init([bad], #aggr_min{})
    )
.
-endif.

%% ----------------------------------------------------------------- %%
