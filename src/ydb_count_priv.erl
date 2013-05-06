%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc Privacy-preserving module for the COUNT aggregate function. Tracks
%%      the number of non-null values seen so far.
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
  , curr_l :: {integer(), number()}
  , curr_m={0, 0} :: {integer(), number()}
  , epsilon=0.01 :: number()
}).

-type aggr_count() :: #aggr_count{
    column :: undefined | atom() | {atom(), atom()}
  , index :: undefined | integer()
  , curr_l :: undefined | {integer(), number()}
  , curr_m :: {integer(), number()}
  , epsilon :: number()
}.
%% Internal count aggregate state.

%-record(mech_state, {time :: integer(), value :: number()}).

%-type mech_state() :: #mech_state{
%    time :: integer()
%  , value :: number()
%}.
%% Internal mechanism state.

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
%% TODO add option to specify epsilon
init(Args) when is_list(Args) -> init(Args, #aggr_count{});

init(_Args) -> {error, {badarg, not_options_list}}.

-spec delegate(Request :: atom(), State :: aggr_count()) ->
    {ok, State :: aggr_count()}
.

%% @private
%% @doc Passes the new count down to its subscribers.
delegate(
    _Request = {tuple, Tuple}
  , State = #aggr_count{
        index=Index, curr_l=CurrL, curr_m = CurrM, epsilon = Eps}
) ->
    {NewL, NewM} = check_tuple(Tuple, Index, CurrL, CurrM, Eps)
  , NewState = State#aggr_count{curr_l=NewL, curr_m=NewM}
  , {ok, NewState}
;

delegate(
    _Request = {tuples, Tuples}
  , State = #aggr_count{
        index=Index, curr_l=CurrL, curr_m = CurrM, epsilon = Eps}
) ->
    {NewL, NewM} = lists:foldl(
        fun(Tuple, {L, M}) ->
            check_tuple(Tuple, Index, L, M, Eps)
        end
      , {CurrL, CurrM}
      , Tuples
    )
  , NewState = State#aggr_count{curr_l=NewL, curr_m=NewM}
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

%% -spec TODO

%% @doc Combines the state of the Laplace and Binary mechanism
%%      to obtain the current count (as an integer).
get_count(_Lap={_LTime, LVal}, _Bin={_MTime, MVal}) ->
    round(LVal) + round(MVal)
.

%% ----------------------------------------------------------------- %%

-spec check_tuple(
    Tuple :: ydb_plan_node:ydb_tuple()
  , Index :: integer()
  , CurrL :: undefined | {integer(), number()} % mech_state()
  , CurrM :: {integer(), number()} % mech_state()
  , Epsilon :: number()
) ->
    %{NewL :: mech_state(), NewM :: mech_state()}
    {NewL :: {integer(), number()}, NewM :: {integer(), number()}}
.

%% @private
%% @doc Selects only the necessary column required for tracking the
%%      count and updates the current count if necessary.
check_tuple(
    Tuple=#ydb_tuple{timestamp=Timestamp}
  , Index
  , undefined
  , CurrM
  , Epsilon
) ->
    check_tuple(Tuple, Index, {Timestamp-1, 0}, CurrM, Epsilon)
;

check_tuple(
    Tuple=#ydb_tuple{data=Data, timestamp=Timestamp}
  , Index
  , CurrL
  , CurrM
  , Epsilon
) ->
    RelevantData = element(Index, Data)
  , NewCount = update_count(get_count(CurrL, CurrM), RelevantData)
  , NewL = do_laplace_advance(CurrL, {Timestamp, NewCount}, Epsilon)
  , NewM = do_binary_advance(CurrM, {Timestamp, NewCount}, NewL, Epsilon)
  , NoisyCount = get_count(NewL, NewM)
  , NewTuple = Tuple#ydb_tuple{data=list_to_tuple([NoisyCount])}
  , ydb_plan_node:notify(
        erlang:self()
      , {tuple, NewTuple}
    )
  , {NewL, NewM}
.

%% ----------------------------------------------------------------- %%

%% -spec TODO

%% @doc Returns value based on relative position of T2 and the current
%%      logarithmic interval in consideration.
find_case(NextPower, T2) when T2 < NextPower->
    1 % T2 is in this interval
;
find_case(NextPower, T2) when T2 == NextPower->
    2 % T2 at the border of this interval
;
find_case(NextPower, T2) when T2 < 2 * NextPower ->
    3 % T2 is in next interval
;
find_case(_NextPower, _T2) ->
    4 % T2 is past next interval
.

%% ----------------------------------------------------------------- %%

%% -spec TODO

%% @doc TODO
%% TODO: should this round the counts here ???
%%       do that thing david talked about ???? (seems annoying though)
%        round counts when we return the value (is what I like the best)
do_laplace_advance(Curr={T1, V1}, New={T2, V2}, Eps) ->
    Gnp1 = ydb_private_utils:get_next_power(T1)
  , case find_case(Gnp1, T2) of
        1 -> Curr
      ; 2 -> {Gnp1, apply_log_mech(V2, Eps)}
      ; 3 -> {Gnp1, apply_log_mech(V1, Eps)}
      ; 4 -> do_laplace_advance({Gnp1, apply_log_mech(V1, Eps)}, New, Eps)
    end
.

%% ----------------------------------------------------------------- %%

%% -spec TODO

%% @doc Applies logarithmic mechanism to specified value.
apply_log_mech(Value, Eps) ->
    Value + ydb_private_utils:random_laplace(1/Eps)
.

%% ----------------------------------------------------------------- %%

%% -spec TODO

%% @doc TODO
%% TODO can't remember if Eps is included in the laplace paramter
do_binary_advance(_Curr={T1, _V1}, _New={T, V}, _Lap={TL, VL}, _Eps) ->
    {T2, V2} = {T - TL, V - VL}
  , Interval = TL
  , {T2, add_binary_noise(V2, Interval, T2-T1)}
.

%% ----------------------------------------------------------------- %%

%% -spec TODO

%% @doc Adds binary noise of size Lap(1/T). If T = 0, adds Lap(1) noise.
%% TODO is this the right solution for T = 0?
add_binary_noise(Value, _T, 0) ->
    Value
;
add_binary_noise(Value, T, NumSteps) ->
    NewValue = Value + ydb_private_utils:random_laplace(1/T)
  , add_binary_noise(NewValue, T, NumSteps - 1)
.

%% ----------------------------------------------------------------- %%

-spec update_count(Sum :: number(), NewNum :: number()) ->
    NewCount :: number().

%% @doc Increments count if a non-null value is passed in.
update_count(Count, null) ->
    Count
;
update_count(Count, _) ->
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
