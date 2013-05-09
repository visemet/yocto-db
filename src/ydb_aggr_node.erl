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

  , result_name :: atom()
  , result_type :: atom()

    % Evaluates an entry over which aggregates are taken
  , eval_fun :: 'undefined' | fun(([term()]) -> term())
    % Computes the aggregate on a single diff
  , pr_fun :: 'undefined' | fun(([term()]) -> term())

    % Computes the aggregate over all diffs
  , aggr_fun :: 'undefined' | fun(([term()]) -> term())
  , synopsis=new_synopsis() :: ets:tid()
}).

-type aggr() :: #aggr{
    history_size :: pos_integer()
  , grouped :: boolean()

  , schema :: ydb_schema()
  , columns :: [atom()]

  , result_name :: atom()
  , result_type :: atom()

  , eval_fun :: 'undefined' | fun(([term()]) -> term())
  , pr_fun :: 'undefined' | fun(([term()]) -> term())

  , aggr_fun :: 'undefined' | fun(([term()]) -> term())
  , synopsis :: ets:tid()
}.
%% Internal state of aggregate node.

-type option() ::
    {history_size, HistorySize :: pos_integer() | 'infinity'}
  | {grouped, Grouped :: boolean()}
  | {columns, Columns :: [atom()]}
  | {result_name, ResultName :: atom()}
  | {result_type, ResultType :: atom()}
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
compute_schema([InputSchema], #aggr{
    result_name = Name
  , result_type = Type
}) ->
    ydb_plan_node:relegate(erlang:self(), {get_schema, InputSchema})

  , OutputSchema = [{Name, {1, Type}}]
  , {ok, OutputSchema}
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

init([{result_name, ResultName} | Args], State = #aggr{}) ->
    init(Args, State#aggr{result_name=ResultName})
;

init([{result_type, ResultType} | Args], State = #aggr{}) ->
    init(Args, State#aggr{result_type=ResultType})
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

-spec new_synopsis() -> Synopsis :: ets:tid().

%% @doc Returns a new ETS table used for storing partial results.
new_synopsis() ->
    {ok, Tid} = ydb_ets_utils:create_table(synopsis)
  , Tid
.

%% ----------------------------------------------------------------- %%

-spec do_update(Tuples :: [ydb_tuple()], State :: aggr()) ->
    {PrevAggr :: term(), CurrAggr :: term()}
.

%% @doc Performs the update to the synopsis based on the received list
%%      of tuples. Returns the previous and current aggregate.
do_update(Tuples, #aggr{
    history_size = HistorySize
  , schema = Schema
  , columns = Columns
  , result_name = ResultName
  , eval_fun = EvalFun
  , pr_fun = PartialFun
  , aggr_fun = AggrFun
  , synopsis = Synopsis
}) when
    is_list(Tuples)
  ->
    Partials = get_partials(Synopsis, ResultName)

  , NewPartial = PartialFun(
        evaluate_tuples(Tuples, EvalFun, {Columns, Schema})
    )
  , add_partial(Synopsis, ResultName, NewPartial)

  , NewPartials = case should_evict(Partials, HistorySize) of
        true ->
            remove_partial(Synopsis, ResultName, erlang:hd(Partials))
          , lists:append(erlang:tl(Partials), [NewPartial])

      ; false -> lists:append(Partials, [NewPartial])
    end

  , PrevAggr = AggrFun(Partials)
  , CurrAggr = AggrFun(NewPartials)

  , {PrevAggr, CurrAggr}
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

-spec should_evict(
    Partials :: [term()]
  , HistorySize :: pos_integer() | 'infinity'
) ->
    boolean()
.

%% @doc Returns `true' if the first partial result is no longer
%%      necessary, and `false' otherwise.
should_evict(Partials, HistorySize) ->
    NumPartials = erlang:length(Partials)

  , if
        HistorySize =:= 'infinity' -> false

      ; NumPartials < HistorySize -> false

      ; NumPartials >= HistorySize -> true
    end
.


%% ----------------------------------------------------------------- %%

-spec get_partials(Synopsis :: ets:tid(), ResultName :: atom()) ->
    Partials :: [term()]
.

%% @doc Returns the list of partial results stored in the synopsis.
get_partials(Synopsis, ResultName) ->
    lists:append(ets:match(Synopsis, {{ResultName, '_'}, '$1'}))
.

-spec add_partial(
    Synopsis :: ets:tid()
  , ResultName :: atom()
  , Partial :: term()
) ->
    ok
.

%% @doc Inserts the partial result into the synopsis.
add_partial(Synopsis, ResultName, Partial) ->
    NextNum = case ets:last(Synopsis) of
        '$end_of_table' -> 1

      ; {ResultName, PrevNum} -> PrevNum + 1
    end

  , ets:insert(Synopsis, {{ResultName, NextNum}, Partial})
  , ok
.

-spec remove_partial(
    Synopsis :: ets:tid()
  , ResultName :: atom()
  , Partial :: term()
) ->
    ok
.

%% @doc Deletes the partial result from the synopsis.
remove_partial(Synopsis, ResultName, Partial) ->
    Num = erlang:hd(ets:match(Synopsis, {{ResultName, '$1'}, Partial}))

  , ets:delete(Synopsis, {ResultName, Num})
  , ok
.

%% ----------------------------------------------------------------- %%
