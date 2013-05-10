%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc Privacy-preserving module for the COUNT aggregate function.
%%      Tracks the number of non-null values seen so far. Implements
%%      the Hybrid Mechanism outlined in 'Private and Continual
%%      Release of Statistics' (Chan, Shi, Song). The bounded mechanism
%%      used to initialize the Hybrid Mechanism can be either the
%%      Binary Mechanism or Simple Counting Mechanism II (both also from
%%      the same paper). Note that the Binary Mechanism is not
%%      pan-private, while Simple Counting Mechanism II is. Most of
%%      the logic is in ydb_private_utils.
-module(ydb_count_priv).
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
  , curr_t=0 :: integer() % little t
  , curr_l=0 :: number() % L(t)
  , curr_lt=0 :: number() % L(T)
  , curr_m :: {number()} | {number(), dict()} % M(\tau)
  , epsilon=0.01 :: number()
  , init_time :: integer()
  , bounded_mech=binary :: atom() % binary | simple_count_II
}).

-type aggr_count() :: #aggr_count{
    column :: undefined | atom() | {atom(), atom()}
  , index :: undefined | integer()
  , curr_t :: integer()
  , curr_l :: number()
  , curr_lt :: number()
  , curr_m :: undefined | {number()} | {number(), dict()}
  , epsilon :: number()
  , init_time :: undefined | integer()
  , bounded_mech :: atom()
}.
%% Internal count aggregate state.

-type option() ::
    {column, Column :: atom() | {ColName :: atom(), NewName :: atom()}}
  | {epsilon, Epsilon :: number()} | {bounded_mech, Mech :: atom()}.
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
  , State = #aggr_count{}
) ->
    NewState = check_tuple(Tuple, State)
  , {ok, NewState}
;

delegate(
    _Request = {tuples, Tuples}
  , State = #aggr_count{}
) ->
    NewState = lists:foldl(
        fun(Tuple, S) ->
            check_tuple(Tuple, S)
        end
      , State
      , Tuples
    )
  %, NewState = State#aggr_count{curr_l=NewL, curr_t=NewT, curr_m=NewM}
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
    post_init(State)
;

init([{column, Column} | Args], State = #aggr_count{}) ->
    init(Args, State#aggr_count{column=Column})
;

init([{epsilon, Epsilon} | Args], State = #aggr_count{}) ->
    init(Args, State#aggr_count{epsilon=Epsilon})
;

init([{bounded_mech, Mech} | Args], State = #aggr_count{}) ->
    init(Args, State#aggr_count{bounded_mech=Mech})
;

init([Term | _Args], #aggr_count{}) ->
    {error, {badarg, Term}}
.

post_init(State = #aggr_count{bounded_mech=BoundedMech}) ->
    case BoundedMech of
        binary -> {ok, State#aggr_count{curr_m={0, dict:new()}}}
      ; simple_count_II -> {ok, State#aggr_count{curr_m={0}}}
    end
.


%% ----------------------------------------------------------------- %%

-spec check_tuple(
    Tuple :: ydb_plan_node:ydb_tuple()
  , State :: aggr_count()
) -> NewState :: aggr_count().

%% @private
%% @doc Selects only the necessary column required for tracking the
%%      count and updates the current count if necessary.
check_tuple(
    Tuple=#ydb_tuple{timestamp=Timestamp}
  , State = #aggr_count{init_time=undefined}
) ->
    check_tuple(Tuple, State#aggr_count{init_time=Timestamp-1})
;

check_tuple(
    Tuple = #ydb_tuple{timestamp=Timestamp, data=Data}
  , State = #aggr_count{
        index=Index
      , curr_l=CurrL
      , curr_t=CurrTime
      , curr_m=CurrMState
      , curr_lt = CurrLT
      , epsilon=Epsilon
      , init_time=InitTime
      , bounded_mech=BoundedMech}
) ->
    RelevantData = element(Index, Data)
  , NewTime = Timestamp - InitTime
  , Sigma = get_sigma(RelevantData)
  , {NewL, NewLT} = ydb_private_utils:do_logarithmic_advance(
        {CurrL, CurrLT}, CurrTime, NewTime, Sigma, Epsilon/2)
  , NewMState = ydb_private_utils:do_bounded_advance(
        CurrMState, CurrTime, NewTime, Sigma, Epsilon/2, BoundedMech)
  , NoisyCount = round(NewLT + element(1, NewMState))
  , NewTuple = Tuple#ydb_tuple{data=list_to_tuple([NoisyCount])}
  , ydb_plan_node:notify(
        erlang:self()
      , {tuple, NewTuple}
    )
  , State#aggr_count{
        curr_l=NewL, curr_lt=NewLT, curr_t=NewTime, curr_m=NewMState}
.

%% ----------------------------------------------------------------- %%

-spec get_sigma(NewNum :: number()) ->
    NewCount :: 0 | 1.

%% @doc Returns 0 if the value is null, and 1 if it is not.
get_sigma(null) -> 0;
get_sigma(_) -> 1.

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
  , ?assertMatch(
        {ok, #aggr_count{epsilon=0.01}}
      , init([], #aggr_count{epsilon=0.01})
    )
  , ?assertMatch(
        {ok, #aggr_count{column=[second], epsilon=0.02}}
      , init([], #aggr_count{column=[second], epsilon=0.02})
    )
.
-endif.

%% ----------------------------------------------------------------- %%
