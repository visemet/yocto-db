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
  , tid :: ets:tid()
}).

-type aggr_min() :: #aggr_min{
    column :: undefined | atom() | {atom(), atom()}
  , index :: undefined | integer()
  , tid :: undefined | ets:tid()}.
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
%% @doc Passes on the tuple to subscribers after it is projected
%%      down.
delegate(
    _Request = {tuple, Tuple}
  , State = #aggr_min{}
) ->
    check_tuple(Tuple, State)
  , {ok, State}
;

delegate(
    _Request = {tuples, Tuples}
  , State = #aggr_min{}
) ->
    lists:foreach(fun(Tuple) ->
        check_tuple(Tuple, State) end, Tuples
    )
  , {ok, State}
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
    {Index, NewSchema} = compute_new_schema(Schema, Column)
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
%%      the project node.
init([], State = #aggr_min{}) ->
    NewState = post_init(State)
  , {ok, NewState}
;

init([{column, Column} | Args], State = #aggr_min{}) ->
    init(Args, State#aggr_min{column=Column})
;

init([Term | _Args], #aggr_min{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

-spec post_init(State :: aggr_min()) -> NewState :: aggr_min().

%% @private
%% @doc Sets up the ETS table for intermediate results.
post_init(State) ->
    {ok, Tid} = ydb_ets_utils:create_table(aggr_min)
  , NewState = State#aggr_min{tid=Tid}
  % TODO create entry?
  , NewState
.

-spec compute_new_schema(
    Schema :: ydb_plan_node:ydb_schema()
  , Column :: atom() | {atom(), atom()}
) ->
    {Index :: integer(), NewSchema :: ydb_plan_node:ydb_schema()}
.

%% @private
%% @doc Computes the new schema.
compute_new_schema(Schema, {OldCol, NewCol}) ->
    {Index, Type} = get_col(OldCol, dict:from_list(Schema))
  , NewSchema = [{NewCol, {1, Type}}]
  , {Index, NewSchema}
;

compute_new_schema(Schema, Column) ->
    compute_new_schema(Schema, {Column, Column})
.

-spec check_tuple(
    Tuple :: ydb_plan_node:ydb_tuple()
  , State :: aggr_min()
) -> ok.

%% @private
%% @doc Selects only the necessary column required for finding the min
%%      and checks to see if it is indeed the minimum.
check_tuple(
    Tuple=#ydb_tuple{data=Data}
  , _State=#aggr_min{index=Index}
) ->
    RelevantData = element(Index, Data)
    , Min = 3
  %, Min = ydb_ets_utils: ? % TODO ets table stuff
  , NewMin = min(Min, RelevantData)
  , NewTuple = Tuple#ydb_tuple{data=list_to_tuple([NewMin])}
  , ydb_plan_node:notify(
        erlang:self()
      , {tuple, NewTuple}
    )
.

%% ----------------------------------------------------------------- %%

-spec get_col(
    ColName :: atom()
  , Schema :: dict())
->
    {Index :: integer(), Type :: atom()} | error.

%% @doc Gets the index and of a particular column.
get_col(ColName, Schema) ->
    case dict:find(ColName, Schema) of
        {ok, {Index, Type}} ->
            {Index, Type}
      ; error -> error
    end
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
