%% @author Max Hirschhorn <maxh@caltech.edu>

%% @doc TODO
-module(ydb_aggr_node).
-behaviour(ydb_plan_node).

-export([start_link/2, start_link/3]).
-export([init/1, delegate/2, delegate/3, compute_schema/2]).

%% @headerfile "ydb_plan_node.hrl"
-include("ydb_plan_node.hrl").

-record(aggr, {
    history_size=1 :: pos_integer() | 'infinity'
  , grouped=false :: boolean()

  , schema=[] :: ydb_schema()
  , columns=[] :: [atom()]

    % Evaluates an entry over which aggregates are taken
  , eval_fun :: 'undefined' | fun(([term()]) -> term())
    % Computes the aggregate on a single diff
  , pr_fun :: 'undefined' | fun(([term()]) -> term())

    % Computes the aggregate over all diffs
  , aggr_fun :: 'undefined' | fun(([term()]) -> term())
  , synopsis :: 'undefined' | ets:tid()
}).

-type aggr() :: #aggr{
    history_size :: pos_integer()
  , grouped :: boolean()

  , schema :: ydb_schema()
  , columns :: [atom()]

  , eval_fun :: 'undefined' | fun(([term()]) -> term())
  , pr_fun :: 'undefined' | fun(([term()]) -> term())

  , aggr_fun :: 'undefined' | fun(([term()]) -> term())
  , synopsis :: 'undefined' | ets:tid()
}.
%% Internal state of aggregate node.

-type option() ::
    {history_size, HistorySize :: pos_integer() | 'infinity'}
  | {grouped, Grouped :: boolean()}
  | {columns, Columns :: [atom()]}
  | {eval_fun, EvalFun :: fun(([term()]) -> term())}
  | {pr_fun, PartialFun :: fun(([term()]) -> term())}
  | {aggr_fun, AggrFun :: fun(([term()]) -> term())}
.
%% TODO

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-spec start_link(
    Args :: [option()]
  , Options :: list()
) ->
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
    {ok, State :: aggr()}
  | {error, {badarg, Term :: term()}}
.

%% @doc Initializes the internal state of the aggregate node.
init(Args) when is_list(Args) -> init(Args, #aggr{}).

-spec delegate(Request :: term(), State :: aggr()) ->
    {ok, NewState :: aggr()}
.

%% @doc TODO
delegate({tuples, Tuples}, State = #aggr{
    schema = Schema
  , columns = Columns
  , eval_fun = EvalFun
}) when
    is_list(Tuples)
  ->
    _Values = evaluate_tuples(Tuples, EvalFun, {Columns, Schema})

  , {ok, State}
;

delegate({diffs, Diffs}, State = #aggr{}) ->
    NewState = lists:foldl(
        fun (Diff, S0 = #aggr{}) ->
            Tuples = get_inserts(Diff)
          , {ok, S1} = delegate({tuples, Tuples}, S0)

          , S1
        end

      , State
      , Diffs
    )

  , {ok, NewState}
;

delegate({get_schema, Schema}, State = #aggr{}) ->
    NewState = State#aggr{schema=Schema}

  , {ok, NewState}
;

delegate(_Request = {info, Message}, State = #aggr{}) ->
    delegate(Message, State)
;

delegate(_Request, State) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec delegate(
    Request :: term()
  , State :: aggr()
  , Extras :: list()
) ->
    {ok, NewState :: aggr()}
.

%% @doc Does nothing.
delegate(_Request, State, _Extras) ->
    {ok, State}
.

-spec compute_schema(
    InputSchemas :: [ydb_plan_node:ydb_schema()]
  , State :: aggr()
) ->
    {ok, OutputSchema :: ydb_plan_node:ydb_schema()}
  | {error, {badarg, InputSchemas :: [ydb_plan_node:ydb_schema()]}}
.

%% @doc Returns the output schema of the aggregate node based upon the
%%      supplied input schemas. Expects a single schema.
compute_schema([Schema], #aggr{}) ->
    ydb_plan_node:relegate(erlang:self(), {get_schema, Schema})

  , {ok, Schema}
;

compute_schema(Schemas, #aggr{}) ->
    {error, {badarg, Schemas}}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: aggr()) ->
    {ok, NewState :: aggr()}
  | {error, Reason :: term()}
.

%% @private
%% @doc Initializes the internal state of the aggregate node.
init([], State = #aggr{}) ->
    {ok, State}
;

init([{history_size, HistorySize} | Args], State = #aggr{}) ->
    init(Args, State#aggr{history_size=HistorySize})
;

init([{grouped, Grouped} | Args], State = #aggr{}) ->
    init(Args, State#aggr{grouped=Grouped})
;

init([{columns, Columns} | Args], State = #aggr{}) ->
    init(Args, State#aggr{columns=Columns})
;

init([{eval_fun, EvalFun} | Args], State = #aggr{}) ->
    init(Args, State#aggr{eval_fun=EvalFun})
;

init([{pr_fun, PartialFun} | Args], State = #aggr{}) ->
    init(Args, State#aggr{pr_fun=PartialFun})
;

init([{aggr_fun, AggrFun} | Args], State = #aggr{}) ->
    init(Args, State#aggr{aggr_fun=AggrFun})
;

init([Term | _Args], #aggr{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

-spec get_inserts(Diff :: ets:tid()) -> Tuples :: [ydb_tuple()].

%% @doc Returns a list of PLUS (`+') tuples from the diff.
get_inserts(Diff) ->
    {Plus, _Minus} = ydb_ets_utils:extract_diffs([Diff])
  , Plus
.

%% ----------------------------------------------------------------- %%

-spec extract_values(
    Tuple :: ydb_tuple()
  , Columns :: [atom()]
  , Schema :: ydb_schema()
) ->
    [term()]
.

%% @doc Extracts the values corresponding to the column names from the
%%      tuple.
extract_values(Tuple, Columns, Schema) ->
    SchemaD = dict:from_list(Schema)

  , lists:map(
        fun (Column) when is_atom(Column) ->
            {Index, _Type} = dict:fetch(Column, SchemaD)
          , erlang:element(Index, Tuple)
        end

      , Columns
    )
.

-spec evaluate_tuples(
    Tuples :: [ydb_tuple]
  , EvalFun :: fun(([term()]) -> term())
  , {Columns :: [atom()], Schema :: ydb_schema()}
) ->
    [term()]
.

%% @doc Evaluates the tuples using the specified function.
evaluate_tuples(
    Tuples
  , EvalFun
  , {Columns, Schema}
) when
    is_list(Tuples)
  , is_function(EvalFun, 1)
  ->
    lists:map(
        fun (#ydb_tuple{data = Data}) ->
            Values = extract_values(Data, Columns, Schema)
          , EvalFun(Values)
        end

      , Tuples
    )
.

%% ----------------------------------------------------------------- %%
