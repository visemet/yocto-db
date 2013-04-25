%% @author Angela Gong <anjoola@anjoola.com>

%% @doc Module for the STDDEV aggregate function. Finds the standard
%%      deviation of the data so far.
-module(ydb_stddev).
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

% curr_M2 is M2 as found in the online_variance function as described in
% http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#
%     On-line_algorithm
-record(aggr_stddev, {
    column :: atom() | {atom(), atom()}
  , index :: integer()
  , curr_count=0 :: integer()
  , curr_M2=0.0 :: float()
  , curr_mean :: float()
}).

-type aggr_stddev() :: #aggr_stddev{
    column :: undefined | atom() | {atom(), atom()}
  , index :: undefined | integer()
  , curr_count :: integer()
  , curr_M2 :: float()
  , curr_mean :: undefined | float()}.
%% Internal stddev aggregate state.

-type option() ::
    {column, Column :: atom() | {ColName :: atom(), NewName :: atom()}}.
%% Options for the STDDEV aggregate:
%% <ul>
%%   <li><code>{column, Column}</code> - The column name to find the
%%       standard deviation of. <code>Column</code> is either an atom
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
    {ok, State :: aggr_stddev()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Initializes the aggregate node's internal state.
init(Args) when is_list(Args) -> init(Args, #aggr_stddev{});

init(_Args) -> {error, {badarg, not_options_list}}.

-spec delegate(Request :: atom(), State :: aggr_stddev()) ->
    {ok, State :: aggr_stddev()}
.

%% @private
%% @doc Passes the new standard deviation down to its subscribers.
delegate(
    _Request = {tuple, Tuple}
  , State = #aggr_stddev{
        curr_count=CurrCount
      , curr_M2=CurrM2
      , curr_mean=CurrMean
      , index=Index
    }
) ->
    {NewCount, NewM2, NewMean} =
        check_tuple(Tuple, Index, CurrCount, CurrM2, CurrMean)
  , NewState = State#aggr_stddev{
        curr_count = NewCount
      , curr_M2 = NewM2
      , curr_mean = NewMean
    }
  , {ok, NewState}
;

delegate(
    _Request = {tuples, Tuples}
  , State = #aggr_stddev{
        curr_count=CurrCount
      , curr_M2=CurrM2
      , curr_mean=CurrMean
      , index=Index
    }
) ->
    {NewCount, NewM2, NewMean} = lists:foldl(
        fun(Tuple, {Count, M2, Mean}) ->
            check_tuple(Tuple, Index, Count, M2, Mean)
        end
      , {CurrCount, CurrM2, CurrMean}
      , Tuples
    )
  , NewState = State#aggr_stddev{
        curr_count = NewCount
      , curr_M2 = NewM2
      , curr_mean = NewMean
    }
  , {ok, NewState}
;

%% @doc Receives the new set of valid indexes and sets it as part
%%      of the state.
delegate(_Request = {index, Index}, State = #aggr_stddev{}) ->
    NewState = State#aggr_stddev{index=Index}
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
  , State :: aggr_stddev()
  , Extras :: list()
) ->
    {ok, NewState :: aggr_stddev()}
.

delegate(_Request, State, _Extras) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec compute_schema(
    InputSchemas :: [ydb_plan_node:ydb_schema()]
  , State :: aggr_stddev()
) ->
    {ok, OutputSchema :: ydb_plan_node:ydb_schema()}
  | {error, {badarg, InputSchemas :: [ydb_plan_node:ydb_schema()]}}
.

%% @doc Returns the output schema of the stddev aggregate based upon
%%      the supplied input schemas. Expects a single schema.
compute_schema([Schema], #aggr_stddev{column=Column}) ->
    {Index, NewSchema} =
        ydb_aggr_utils:compute_new_schema(Schema, Column, "STDDEV")
    % Inform self of index to check for.
  , ydb_plan_node:relegate(
        erlang:self()
      , {index, Index}
    )
  , {ok, NewSchema}
;   

compute_schema(Schemas, #aggr_stddev{}) ->
    {error, {badarg, Schemas}}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: aggr_stddev()) ->
    {ok, State :: aggr_stddev()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Parses initializing arguments to set up the internal state of
%%      the standard deviation aggregate node.
init([], State = #aggr_stddev{}) ->
    {ok, State}
;

init([{column, Column} | Args], State = #aggr_stddev{}) ->
    init(Args, State#aggr_stddev{column=Column})
;

init([Term | _Args], #aggr_stddev{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

-spec check_tuple(
    Tuple :: ydb_plan_node:ydb_tuple()
  , Index :: integer()
  , CurrCount :: integer()
  , CurrM2 :: float()
  , CurrMean :: float()
) -> {NewCount :: integer(), NewM2 :: float(), NewMean :: float()}.

%% @private
%% @doc Selects only the necessary column required for finding the 
%%      stddev and computes the new standard deviation. Updates the 
%%      current count, M2, and mean.
check_tuple(
    Tuple=#ydb_tuple{data=Data}
  , Index
  , CurrCount
  , CurrM2
  , CurrMean
) ->
    RelevantData = element(Index, Data)
  , {NewM2, NewMean, StdDev} =
        get_stddev(CurrCount, CurrM2, CurrMean, RelevantData)
  , NewTuple = Tuple#ydb_tuple{data=list_to_tuple([StdDev])}
  , ydb_plan_node:notify(
        erlang:self()
      , {tuple, NewTuple}
    )
  , {CurrCount + 1, NewM2, NewMean}
.    

-spec get_stddev(
    CurrCount :: integer()
  , CurrM2 :: float()
  , CurrMean :: float()
  , NewNum :: number())
-> {NewM2 :: float(), NewMean :: float(), StdDev :: float()}.
    
%% @doc Gets the standard deviation.
get_stddev(0, _CurrM2, _CurrMean, NewNum) ->
   {0, NewNum, 0}
;

get_stddev(CurrCount, CurrM2, CurrMean, NewNum) ->
    NewCount = CurrCount + 1
  , Delta = NewNum - CurrMean
  , NewMean = CurrMean + Delta / NewCount
  , NewM2 = CurrM2 + Delta * (NewNum - NewMean)
  , Var = NewM2 / CurrCount
  , {NewM2, NewMean, math:sqrt(Var)}
.

%%% =============================================================== %%%
%%%  private tests                                                  %%%
%%% =============================================================== %%%

-ifdef(TEST).
init_test() ->
    ?assertMatch(
        {ok, #aggr_stddev{column=[first]}}
      , init([], #aggr_stddev{column=[first]})
    )
  , ?assertMatch(
        {error, {badarg, bad}}
      , init([bad], #aggr_stddev{})
    )
.
-endif.

%% ----------------------------------------------------------------- %%
