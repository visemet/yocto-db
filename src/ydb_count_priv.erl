%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc Privacy-preserving module for the COUNT aggregate function.
%%      Tracks the number of non-null values seen so far.
%%
%%      Details for the mechanism are as follows:
%%
%%      Let L be the logarithmic mechanism, which is a time-unbounded
%%      mechanism that outputs the noisy count L(t, \epsilon). It adds
%%      noise to the count at every timestep that is a power of 2. This
%%      mechanism guarantees \epsilon-differential privacy when used
%%      alone.
%%
%%      Let M be any time-bounded mechanism, which outputs the noisy
%%      count M(t, \epsilon). It adds noise to the count as specified,
%%      and guarantees \epsilon-differential privacy when used alone.
%%
%%      The hybrid mechanism, which is used in this module, combines L
%%      and M by using L to track the noisy count at powers of 2, and
%%      M to track the count in the intervals between the powers of 2.
%%
%%      Let t' be the largest power of two that is less than t, and let
%%      \tau be t' - t. (So if t = 10, t' = 8 and \tau = 2). The count
%%      C(t) at timestep t is given by C(t) = L(t') + M(\tau). At every
%%      timestep that is a power of 2, M is reset to 0.
%%
%%      Accordingly, This implementation keeps track of three mechanism
%%      state values:
%%          currL = {t, L(t)}
%%          currM = {\tau, M(\tau)}
%%          currT = {t', L(t')}
%%
%%      Finally, since all the papers assume that the stream sigma is a
%%      stream of 0's or 1's for each timestep, but realistically, we
%%      only receive tuples for the 1's in the stream, the
%%      implementation has to take into account the fact that the
%%      timestamp is not incremented by 1 each time.
%%
%%      So, when a tuple is received, check_tuple/2 is called.
%%      check_tuple/2 fastforwards the mechanism state variables from
%%      the previous time t0 to the current time t, essentially adding
%%      noise as many times as necessary between t0 and t. It then
%%      outputs a tuple with the new noisy count, and returns the
%%      modified state.
-module(ydb_count_priv).
-behaviour(ydb_plan_node).

-export([start_link/2, start_link/3]).
-export([init/1, delegate/2, delegate/3, compute_schema/2]).

%% @headerfile "ydb_plan_node.hrl"
-include("ydb_plan_node.hrl").
%-include("logging.hrl").

% Testing for private functions.
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%% =============================================================== %%%
%%%  internal records and types                                     %%%
%%% =============================================================== %%%

-record(mech_state, {
    time :: integer()
  , value=0 :: number()
}).

-type mech_state() :: #mech_state{
    time :: undefined | integer()
  , value :: number()
}.
%% Internal mechanism state.

-record(aggr_count, {
    column :: atom() | {atom(), atom()}
  , index :: integer()
  , curr_l :: mech_state()
  , curr_t :: mech_state()
  , curr_m = #mech_state{time=0, value=0} :: mech_state()
  , epsilon=0.01 :: number()
  , init_time :: integer()
}).

-type aggr_count() :: #aggr_count{
    column :: undefined | atom() | {atom(), atom()}
  , index :: undefined | integer()
  , curr_l :: undefined | mech_state()
  , curr_t :: undefined | mech_state()
  , curr_m :: mech_state()
  , epsilon :: number()
  , init_time :: undefined | integer()
}.
%% Internal count aggregate state.

-type option() ::
    {column, Column :: atom() | {ColName :: atom(), NewName :: atom()}}
  | {epsilon, Epsilon :: number()}.
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
    {ok, State}
;

init([{column, Column} | Args], State = #aggr_count{}) ->
    init(Args, State#aggr_count{column=Column})
;

init([{epsilon, Epsilon} | Args], State = #aggr_count{}) ->
    init(Args, State#aggr_count{epsilon=Epsilon})
;

init([Term | _Args], #aggr_count{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

-spec get_count(Lap :: mech_state(), Bin :: mech_state()) ->
    integer()
.

%% @doc Combines the state of the Laplace and Binary mechanism
%%      to obtain the current count (as an integer).
get_count(Lap, Bin) ->
    round(Lap#mech_state.value) + round(Bin#mech_state.value)
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
  , State = #aggr_count{
        curr_l=undefined
      , curr_t=undefined
      , init_time=undefined}
) ->
    RelTime = get_relative_time(undefined, Timestamp)
  , NewL = #mech_state{time=RelTime, value=0}
  , NewT = #mech_state{time=ydb_private_utils:get_prev_power(RelTime), value=0}
  , check_tuple(Tuple,
        State#aggr_count{curr_l=NewL, curr_t=NewT, init_time=Timestamp-1})
;

check_tuple(
    Tuple = #ydb_tuple{timestamp=Timestamp, data=Data}
  , State = #aggr_count{
        index=Index
      , curr_l=CurrL
      , curr_t=CurrT
      , curr_m=CurrM
      , epsilon=Epsilon
      , init_time=InitTime}
) ->
    RelevantData = element(Index, Data)
  , RelTimestamp = get_relative_time(InitTime, Timestamp)
  , Stream = get_stream(RelevantData)
  , NewState = #mech_state{time=RelTimestamp, value=Stream}
  , {NewT, NewL} = ydb_private_utils:do_logarithmic_advance(
        CurrL, CurrT, NewState, Epsilon)
  , NewM = ydb_private_utils:do_simple_count_II_advance(
        CurrM, NewState, NewT, Epsilon)
  , NoisyCount = get_count(NewT, NewM)
  , NewTuple = Tuple#ydb_tuple{data=list_to_tuple([NoisyCount])}
  %, ?TRACE("we have ~nL = ~p, ~nT = ~p, ~nM = ~p~n", [NewL, NewT, NewM])
  , ydb_plan_node:notify(
        erlang:self()
      , {tuple, NewTuple}
    )
  , State#aggr_count{curr_l=NewL, curr_t=NewT, curr_m=NewM}
.

%% ----------------------------------------------------------------- %%

-spec get_relative_time(
    InitTime :: undefined | integer()
  , Time :: integer()
) -> RelTime :: integer().

%% @doc Returns the time, relative to the init time. If InitTime is
%%      undefined, returns 0.
get_relative_time(undefined, _) -> 0;

get_relative_time(InitTime, Time) -> Time - InitTime.

%% ----------------------------------------------------------------- %%

-spec get_stream(NewNum :: number()) ->
    NewCount :: 0 | 1.

%% @doc Increments count if a non-null value is passed in.
get_stream(null) -> 0;

get_stream(_) -> 1.

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
