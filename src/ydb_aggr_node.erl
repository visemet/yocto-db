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
  , incremental=false :: boolean()
  , grouped=false :: [atom()] | false

  , schema=[] :: ydb_schema()
  , columns=[] :: [atom()]

  , result_name :: atom()
  , result_type :: atom()

    % Evaluates an entry over which aggregates are taken.
  , eval_fun :: 'undefined' | fun(([term()]) -> term())
    % Computes the aggregate on a single diff.
  , pr_fun ::
        'undefined'
      | fun(([term()]) -> term())           % Non-incremental
      | fun(([term()], [term()]) -> term()) % Incremental

    % Computes the aggregate over all diffs.
  , aggr_fun :: 'undefined' | fun(([term()]) -> term())
  , synopsis=new_synopsis() :: ets:tid()

  , prev_result :: term()
  , prev_time=0 :: non_neg_integer()
}).

-type aggr() :: #aggr{
    history_size :: pos_integer()
  , incremental :: boolean()
  , grouped :: [atom()] | false

  , schema :: ydb_schema()
  , columns :: [atom()]

  , result_name :: atom()
  , result_type :: atom()

  , eval_fun :: 'undefined' | fun(([term()]) -> term())
  , pr_fun ::
        'undefined'
      | fun(([term()]) -> term())
      | fun(([term()], [term()]) -> term())

  , aggr_fun :: 'undefined' | fun(([term()]) -> term())
  , synopsis :: ets:tid()

  , prev_result :: term()
  , prev_time :: non_neg_integer()
}.
%% Internal state of aggregate node.

-type option() ::
    {history_size, HistorySize :: pos_integer() | 'infinity'}
  | {incremental, Incremental :: boolean()}
  | {grouped, Grouped :: [atom()] | boolean()}
  | {columns, Columns :: [atom()]}
  | {result_name, ResultName :: atom()}
  | {result_type, ResultType :: atom()}
  | {eval_fun, EvalFun :: fun(([term()]) -> term())}
  | {pr_fun, PartialFun ::
        fun(([term()]) -> term())
      | fun(([term()], [term()]) -> term())
    }
  | {aggr_fun, AggrFun :: fun(([term()]) -> term())}
.
%% Options for the aggregate:
%% <ul>
%%   <li><code>{history_size, HistorySize}</code> - TODO</li>
%%   <li><code>{incremental, Incremental}</code> - Is <code>true</code>
%%       if want computation to be incremental. Is <code>false</code>
%%       otherwise.</li>
%%   <li><code>{grouped, Grouped}</code> - If no grouping id desired,
%%       is equal to <code>false</code>. Otherwise is a list of
%%       columm names to group by.</code></li>
%%   <li><code>{columns, Columns}</code> - Columns to take the 
%%       aggregate on.</li>
%%   <li><code>{result_name, ResultName}</code> - The resulting name
%%       of the aggregate column.</li>
%%   <li><code>{result_type, ResultType}</code> - The resulting type
%%       of the aggregate column.</li>
%%   <li><code>{eval_fun, EvalFun}</code> - TODO</li>
%%   <li><code>{pr_fun, PartialFun}</code> - TODO</li>
%%   <li><code>{aggr_fun, AggrFun}</code> - TODO</li>
%% </ul>

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

%% @doc Processes tuples and diffs by producing a new aggregate value
%%      for each such message received.
delegate({tuples, Tuples}, State = #aggr{}) when is_list(Tuples) ->
    {AggrTuples, NewState} = lists:mapfoldl(
        fun (Tuple = #ydb_tuple{timestamp = Timestamp}, S0 = #aggr{}) ->
            CurrAggr = do_update([Tuple], S0)

          , NewTuple = make_tuple(Timestamp, CurrAggr)
          , S1 = S0#aggr{
                prev_result=CurrAggr
              , prev_time=Timestamp
            }

          , {NewTuple, S1}
        end

      , State
      , Tuples
    )

  , ydb_plan_node:send_tuples(
        erlang:self()
      , lists:filter(
            fun (undefined) -> false

              ; (_Tuple) -> true
            end

          , AggrTuples
        )
    )

  , {ok, NewState}
;

% TODO need to handle groups
delegate({diffs, Diffs}, State = #aggr{
    result_name = ResultName
  , prev_result = PrevResult
  , prev_time = PrevTime
}) when
    is_list(Diffs)
  ->
    {AggrDiffs, NewState} = lists:mapfoldl(
        fun (Diff, S0 = #aggr{}) ->
            Tuples = get_inserts(Diff)
          , CurrAggr = do_update(Tuples, S0)

          , Last = lists:last(Tuples) % Assumes tuples are written in
                                      % ascending order by timestamp.
          , Timestamp = Last#ydb_tuple.timestamp

          , OldTuple = make_tuple(PrevTime, PrevResult)
          , NewTuple = make_tuple(Timestamp, CurrAggr)

          , {ok, Tid} = ydb_ets_utils:create_diff_table(?MODULE)

          , if
                OldTuple =/= undefined ->
                    ydb_ets_utils:add_diffs(Tid, '-', ResultName, OldTuple)

              ; OldTuple =:= undefined -> pass
            end

          , if
                NewTuple =/= undefined ->
                    ydb_ets_utils:add_diffs(Tid, '+', ResultName, NewTuple)

              ; NewTuple =:= undefined -> pass
            end

          , S1 = S0#aggr{
                prev_result=CurrAggr
              , prev_time=Timestamp
            }

          , {Tid, S1}
        end

      , State
      , Diffs
    )

  , ydb_plan_node:send_diffs(erlang:self(), AggrDiffs)

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
  , grouped = false
}) ->
    ydb_plan_node:relegate(erlang:self(), {get_schema, InputSchema})
    % TODO what does the line above do

  , OutputSchema = [{Name, {1, Type}}]
  , {ok, OutputSchema}
;

compute_schema([InputSchema], #aggr{
    result_name = Name
  , result_type = Type
  , grouped = Columns
}) ->
    ydb_plan_node:relegate(erlang:self(), {get_schema, InputSchema})
    % TODO what does the line above do
    
    % Get the part of the schema with just the groups.
  , GroupedSchema = ydb_group:compute_grouped_schema(InputSchema, Columns)
  , NumGroupedCols = length(Columns)
  
    % Add on the aggregate column.
  , OutputSchema = GroupedSchema ++ [{Name, {NumGroupedCols + 1, Type}}]
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

init([{incremental, Incremental} | Args], State = #aggr{}) ->
    % The history size is set to 1 by default.
    init(Args, State#aggr{incremental=Incremental})
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
% TODO: cleanup
  %   {ok, Tid} = ydb_ets_utils:create_table(synopsis)
  % , Tid

    ets:new(synopsis, [ordered_set])
.

-spec make_tuple(
    Timestamp :: non_neg_integer()
  , Data :: term()
) ->
    ydb_plan_node:ydb_tuple() | 'undefined'
.

%% @doc Returns a new tuple with the specified timestamp and data.
make_tuple(_Timestamp, undefined) ->
    undefined
;

make_tuple(Timestamp, Aggr) ->
    #ydb_tuple{timestamp=Timestamp, data={Aggr}}
.

%% ----------------------------------------------------------------- %%

-spec do_update(Tuples :: [ydb_tuple()], State :: aggr()) ->
    CurrAggr :: term()
.

%% @doc Performs the update to the synopsis based on the received list
%%      of tuples. Returns the previous and current aggregate.
do_update(Tuples, #aggr{
    history_size = HistorySize
  , incremental = Incremental
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
  , NewPartial = if
        Incremental =:= true ->
            PartialFun(
                evaluate_tuples(Tuples, EvalFun, {Columns, Schema})
              , Partials
            )

      ; Incremental =:= false ->
            PartialFun(
                evaluate_tuples(Tuples, EvalFun, {Columns, Schema})
            )
    end

  , add_partial(Synopsis, ResultName, NewPartial)

  , case should_evict(Partials, HistorySize) of
        true -> remove_partial(Synopsis, ResultName, erlang:hd(Partials))

      ; false -> pass
    end

    % Technically, for an incremental algorithm, `AggrFun' only needs
    % `NewPartial'.
  , AggrFun(lists:append(Partials, [NewPartial]))
.

%% ----------------------------------------------------------------- %%

-spec get_inserts(Diff :: ets:tid()) -> Tuples :: [ydb_plan_node:ydb_tuple()].

%% @doc Returns a list of PLUS (`+') tuples from the diff.
get_inserts(Diff) ->
    {Plus, _Minus} = ydb_ets_utils:extract_diffs([Diff])
  , Plus
.

%% ----------------------------------------------------------------- %%

-spec extract_values(
    Tuple :: ydb_plan_node:ydb_tuple()
  , Columns :: [atom()]
  , Schema :: ydb_plan_node:ydb_schema()
) ->
    [term()]
.

%% @doc Extracts the values corresponding to the column names from the
%%      tuple.
extract_values(
    _Tuple = #ydb_tuple{timestamp = Timestamp, data = Data}
  , Columns
  , Schema
) when
    is_list(Columns)
  , is_list(Schema)
  ->
    SchemaD = dict:from_list(Schema)

  , lists:map(
        fun ('$timestamp') -> Timestamp

          ; (Column) when is_atom(Column) ->
            case dict:find(Column, SchemaD) of
                {ok, {Index, _Type}} -> erlang:element(Index, Data)

              ; error -> undefined
            end
        end

      , Columns
    )
.

-spec evaluate_tuples(
    Tuples :: [ydb_plan_node:ydb_tuple()]
  , EvalFun :: fun(([term()]) -> term())
  , {Columns :: [atom()], Schema :: ydb_plan_node:ydb_schema()}
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
        fun (Tuple = #ydb_tuple{}) ->
            Values = extract_values(Tuple, Columns, Schema)
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
    Num = erlang:hd(lists:append(
        ets:match(Synopsis, {{ResultName, '$1'}, Partial})
    ))

  , ets:delete(Synopsis, {ResultName, Num})
  , ok
.

%% ----------------------------------------------------------------- %%
