%% @author Angela Gong <anjoola@anjoola.com>

%% @doc Module for the VAR aggregate function. Finds the variance of
%%      the data so far.
-module(ydb_var).
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
-record(aggr_var, {
    column :: atom() | {atom(), atom()}
  , index :: integer()
  , curr_count=0 :: integer()
  , curr_M2=0.0 :: float()
  , curr_mean :: float()
}).

-type aggr_var() :: #aggr_var{
    column :: undefined | atom() | {atom(), atom()}
  , index :: undefined | integer()
  , curr_count :: integer()
  , curr_M2 :: float()
  , curr_mean :: undefined | float()}.
%% Internal var aggregate state.

-type option() ::
    {column, Column :: atom() | {ColName :: atom(), NewName :: atom()}}.
%% Options for the VAR aggregate:
%% <ul>
%%   <li><code>{column, Column}</code> - The column name to find the
%%       variance of. <code>Column</code> is either an atom
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
    {ok, State :: aggr_var()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Initializes the aggregate node's internal state.
init(Args) when is_list(Args) -> init(Args, #aggr_var{});

init(_Args) -> {error, {badarg, not_options_list}}.

-spec delegate(Request :: atom(), State :: aggr_var()) ->
    {ok, State :: aggr_var()}
.

%% @private
%% @doc Passes the new variance down to its subscribers.
delegate(
    _Request = {tuple, Tuple}
  , State = #aggr_var{
        curr_count=CurrCount
      , curr_M2=CurrM2
      , curr_mean=CurrMean
      , index=Index
    }
) ->
    {NewCount, NewM2, NewMean} =
        check_tuple(Tuple, Index, CurrCount, CurrM2, CurrMean)
  , NewState = State#aggr_var{
        curr_count = NewCount
      , curr_M2 = NewM2
      , curr_mean = NewMean
    }
  , {ok, NewState}
;

delegate(
    _Request = {tuples, Tuples}
  , State = #aggr_var{
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
  , NewState = State#aggr_var{
        curr_count = NewCount
      , curr_M2 = NewM2
      , curr_mean = NewMean
    }
  , {ok, NewState}
;

%% @doc Receives the new set of valid indexes and sets it as part
%%      of the state.
delegate(_Request = {index, Index}, State = #aggr_var{}) ->
    NewState = State#aggr_var{index=Index}
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
  , State :: aggr_var()
  , Extras :: list()
) ->
    {ok, NewState :: aggr_var()}
.

delegate(_Request, State, _Extras) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec compute_schema(
    InputSchemas :: [ydb_plan_node:ydb_schema()]
  , State :: aggr_var()
) ->
    {ok, OutputSchema :: ydb_plan_node:ydb_schema()}
  | {error, {badarg, InputSchemas :: [ydb_plan_node:ydb_schema()]}}
.

%% @doc Returns the output schema of the var aggregate based upon the
%%      supplied input schemas. Expects a single schema.
compute_schema([Schema], #aggr_var{column=Column}) ->
    {Index, NewSchema} =
        ydb_aggr_utils:compute_new_schema(Schema, Column, "VAR")
    % Inform self of index to check for.
  , ydb_plan_node:relegate(
        erlang:self()
      , {index, Index}
    )
  , {ok, NewSchema}
;   

compute_schema(Schemas, #aggr_var{}) ->
    {error, {badarg, Schemas}}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: aggr_var()) ->
    {ok, State :: aggr_var()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Parses initializing arguments to set up the internal state of
%%      the min aggregate node.
init([], State = #aggr_var{}) ->
    {ok, State}
;

init([{column, Column} | Args], State = #aggr_var{}) ->
    init(Args, State#aggr_var{column=Column})
;

init([Term | _Args], #aggr_var{}) ->
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
%% @doc Selects only the necessary column required for finding the var
%%      and computes the new variance. Updates the current count, M2,
%%      and mean.
check_tuple(
    Tuple=#ydb_tuple{data=Data}
  , Index
  , CurrCount
  , CurrM2
  , CurrMean
) ->
    RelevantData = element(Index, Data)
  , {NewM2, NewMean, Var} = get_var(CurrCount, CurrM2, CurrMean, RelevantData)
  , NewTuple = Tuple#ydb_tuple{data=list_to_tuple([Var])}
  , ydb_plan_node:notify(
        erlang:self()
      , {tuple, NewTuple}
    )
  , {CurrCount + 1, NewM2, NewMean}
.    

-spec get_var(
    CurrCount :: integer()
  , CurrM2 :: float()
  , CurrMean :: float()
  , NewNum :: number())
-> {NewM2 :: float(), NewMean :: float(), Var :: float()}.
    
%% @doc Gets the variance.
get_var(0, _CurrM2, _CurrMean, NewNum) ->
   {0, NewNum, 0}
;

get_var(CurrCount, CurrM2, CurrMean, NewNum) ->
    NewCount = CurrCount + 1
  , Delta = NewNum - CurrMean
  , NewMean = CurrMean + Delta / NewCount
  , NewM2 = CurrM2 + Delta * (NewNum - NewMean)
  , Var = NewM2 / CurrCount
  , {NewM2, NewMean, Var}
.

%%% =============================================================== %%%
%%%  private tests                                                  %%%
%%% =============================================================== %%%

-ifdef(TEST).
init_test() ->
    ?assertMatch(
        {ok, #aggr_var{column=[first]}}
      , init([], #aggr_var{column=[first]})
    )
  , ?assertMatch(
        {error, {badarg, bad}}
      , init([bad], #aggr_var{})
    )
.
-endif.

%% ----------------------------------------------------------------- %%
