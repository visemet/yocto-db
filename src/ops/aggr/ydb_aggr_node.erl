%% @author Max Hirschhorn, Angela Gong
%%         <maxh@caltech.edu, anjoola@anjoola.com>

%% @doc TODO
-module(ydb_aggr_node).
-behaviour(ydb_plan_node).

-export([start_link/2, start_link/3]).
-export([init/1, delegate/2, delegate/3, compute_schema/2]).

%% @headerfile "ydb_plan_node.hrl"
-include("ydb_plan_node.hrl").

% Testing for private functions.
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(NO_GROUP, false).
-define(is_dict (D), is_tuple(D) andalso is_element(1, D) =:= dict).

%%% =============================================================== %%%
%%%  internal records and types                                     %%%
%%% =============================================================== %%%

-record(aggr, {
    history_size=1 :: pos_integer() | 'infinity'
  , incremental=false :: boolean()
  , grouped=?NO_GROUP :: [atom()] | ?NO_GROUP
    % Indexes of the columns that are to be grouped.
  , group_indexes=[] :: [integer()]

  , schema=[] :: ydb_plan_node:ydb_schema()
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

    % Previous result is a dictionary whose key is the group key and
    % value is the previous result for that group. If there is not
    % grouping, then the key is 'false' and there is only one entry
    % in the dictionary.
  , prev_results=dict:new() :: dict()
    % Latest timestamps.
  , prev_times=dict:new() :: dict()
    % Latest internal ets table timestamps.
  , prev_nums=dict:new() :: dict()
}).

-type aggr() :: #aggr{
    history_size :: pos_integer()
  , incremental :: boolean()
  , grouped :: [atom()] | ?NO_GROUP
  , group_indexes :: [integer()]

  , schema :: ydb_plan_node:ydb_schema()
  , columns :: [atom()]

  , result_name :: 'undefined' | atom()
  , result_type :: 'undefined' | atom()

  , eval_fun :: 'undefined' | fun(([term()]) -> term())
  , pr_fun ::
        'undefined'
      | fun(([term()]) -> term())
      | fun(([term()], [term()]) -> term())

  , aggr_fun :: 'undefined' | fun(([term()]) -> term())
  , synopsis :: ets:tid()

  , prev_results :: dict()
  , prev_times :: dict()
  , prev_nums :: dict()
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
delegate({tuples, Tuples}, State = #aggr{grouped=Grouped})
  when
    is_list(Tuples)
  ->
    NewState = process_tuples(Tuples, Grouped, State)
  , {ok, NewState}
;

delegate({diffs, Diffs}, State = #aggr{grouped=Grouped})
  when
    is_list(Diffs)
  ->
    {AggrDiffs, NewState} = lists:mapfoldl(
        fun (Diff, S0 = #aggr{}) ->
            Tuples = get_inserts(Diff)
          , process_diffs(Tuples, Grouped, S0)
        end
      , State
      , Diffs
    )
  , ydb_plan_node:send_diffs(erlang:self(), AggrDiffs)
  , {ok, NewState}
;

delegate({get_schema, Schema}, State = #aggr{grouped=Grouped}) ->
    % Get the indexes of the grouped columns, if grouping.
    if
        Grouped =/= false ->
            GroupIndexes=ydb_group:get_group_indexes(Schema, Grouped)
          , NewState = State#aggr{schema=Schema, group_indexes=GroupIndexes}
          , {ok, NewState}

      ; Grouped =:= false ->
            NewState = State#aggr{schema=Schema}
          , {ok, NewState}
    end
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
  , grouped = ?NO_GROUP
}) ->
    % Inform self of input schema.
    ydb_plan_node:relegate(erlang:self(), {get_schema, InputSchema})
  , OutputSchema = [{Name, {1, Type}}]
  , {ok, OutputSchema}
;

compute_schema([InputSchema], #aggr{
    result_name = Name
  , result_type = Type
  , grouped = Columns
}) ->
    % Inform self of input schema.
    ydb_plan_node:relegate(erlang:self(), {get_schema, InputSchema})

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
  , GroupKey :: ?NO_GROUP | tuple()
) ->
    ydb_plan_node:ydb_tuple() | 'undefined'
.

%% @doc Returns a new tuple with the specified timestamp and data.
make_tuple(_Timestamp, undefined, _GroupKey) ->
    undefined
;

make_tuple(Timestamp, Aggr, ?NO_GROUP) ->
    #ydb_tuple{timestamp=Timestamp, data={Aggr}}
;

make_tuple(Timestamp, Aggr, GroupKey) ->
    #ydb_tuple{
        timestamp=Timestamp
        % Add the grouped columns to the front first.
      , data=erlang:append_element(GroupKey, Aggr)
    }
.

-spec make_diff_tuple(
    TimeDict :: dict()
  , ResultDict :: dict()
  , GroupKey :: ?NO_GROUP | tuple()
) ->
    ydb_plan_node:ydb_tuple() | 'undefined'
.

make_diff_tuple(TimeDict, ResultDict, GroupKey) ->
    Timestamp = case dict:find(GroupKey, TimeDict) of
        {ok, Time} -> Time

      ; error -> undefined
    end

  , case dict:find(GroupKey, ResultDict) of
        {ok, Result} -> make_tuple(Timestamp, Result, GroupKey)

      ; error -> undefined
    end
.

%% ----------------------------------------------------------------- %%

-spec process_tuples(
    Tuples :: [ydb_plan_node:ydb_tuple()]
  , Grouped :: ?NO_GROUP | tuple() | [tuple()]
  , State :: aggr()
) ->
    NewState :: aggr()
.

%% @doc Process newly-received tuples.
process_tuples(
    Tuples
  , Grouped
  , State=#aggr{group_indexes=GroupIndexes}
)
  when
    is_list(Grouped)
  ->
    % Split tuples into their groups. Is a list of tuples containing
    % the key for the group GroupKey and the list of tuples in that group
    % GroupTuples -> {GroupKey, GroupTuples}.
    ListGroupedTuples = ydb_group:split_tuples(Tuples, GroupIndexes)

    % Process each group separately.
  , NewState = lists:foldl(
        fun({GroupKey, GroupTuples}, NewState) ->
            process_tuples(GroupTuples, GroupKey, NewState)
        end
      , State
      , ListGroupedTuples
    )
  , NewState
;

process_tuples(
    Tuples
  , Grouped
  , State=#aggr{prev_results=PrevResults, prev_times=PrevTimes}
) ->
    {AggrTuples, NewState} = lists:mapfoldl(
        fun (Tuple = #ydb_tuple{timestamp = Timestamp}, S0 = #aggr{}) ->
            {NewNums, CurrAggr} = do_update([Tuple], S0, Grouped)

          , NewTuple = make_tuple(Timestamp, CurrAggr, Grouped)
          , S1 = S0#aggr{
                prev_results=dict:store(Grouped, CurrAggr, PrevResults)
              , prev_times=dict:store(Grouped, Timestamp, PrevTimes)
              , prev_nums=NewNums
            }

          , {NewTuple, S1}
        end
      , State
      , Tuples
    )
    % Send tuples to listeners.
  , ydb_plan_node:send_tuples(
        erlang:self()
      , lists:filter(
            fun (undefined) -> false

              ; (_Tuple) -> true
            end

          , AggrTuples
        )
    )
  , NewState
.

-spec process_diffs(
    Tuples :: [ydb_plan_node:ydb_tuple()]
  , Grouped :: ?NO_GROUP | [tuple()]
  , State :: aggr()
) ->
    {Tid :: ets:tid(), NewState :: aggr()}
.

%% @doc Processes a newly-received diff.
process_diffs(Tuples, ?NO_GROUP, State) ->
    {ok, Tid} = ydb_ets_utils:create_diff_table(?MODULE)
  , NewState = process_diff_group(Tuples, ?NO_GROUP, State, Tid)
  , {Tid, NewState}
;

process_diffs(
    Tuples
  , Grouped
  , State=#aggr{group_indexes=GroupIndexes}
)
  when
    is_list(Tuples)
  , is_list(Grouped)
  ->
  io:format("Tuples: ~w, Grouped: ~w, State: ~w~n~n", [Tuples, Grouped, State]),

    % Splits tuples into their groups. Is a list of tuples containing
    % the key for the group GroupKey and the list of tuples in that group
    % GroupTuples -> {GroupKey, GroupTuples}
    ListGroupedTuples = ydb_group:split_tuples(Tuples, GroupIndexes)

  , {ok, Tid} = ydb_ets_utils:create_diff_table(?MODULE)
    % Process each group separately.
  , NewState = lists:foldl(
        fun({GroupKey, GroupTuples}, NewState) ->
            process_diff_group(GroupTuples, GroupKey, NewState, Tid)
        end
      , State
      , ListGroupedTuples
    )
  , {Tid, NewState}
.

-spec process_diff_group(
    Tuples :: [ydb_plan_node:ydb_tuple()]
  , Grouped :: ?NO_GROUP | tuple()
  , State :: aggr()
  , Tid :: ets:tid()
) ->
    NewState :: aggr()
.

%% @doc Processes a particular group for diffs.
process_diff_group(
    Tuples
  , Grouped
  , State=#aggr{
        result_name = ResultName
      , prev_results = PrevResults
      , prev_times = PrevTimes
    }
  , Tid
) when
    is_list(Tuples)
  ->
    {NewNums, CurrAggr} = do_update(Tuples, State, Grouped)

    % Assumes the tuples are written in ascending order by timestamp.
  , Last = lists:last(Tuples)
  , Timestamp = Last#ydb_tuple.timestamp

    % Inform listeners of new results.
  , OldTuple = make_diff_tuple(PrevTimes, PrevResults, Grouped)
  , NewTuple = make_tuple(Timestamp, CurrAggr, Grouped)
  , if
        OldTuple =/= undefined ->
            ydb_ets_utils:add_diffs(Tid, '-', ResultName, OldTuple)
      ; OldTuple =:= undefined ->
            pass
    end
  , if
        NewTuple =/= undefined ->
            ydb_ets_utils:add_diffs(Tid, '+', ResultName, NewTuple)
      ; NewTuple =:= undefined ->
            pass
    end

    % Update previous results.
  , NewState = State#aggr{
        prev_results=dict:store(Grouped, CurrAggr, PrevResults)
      , prev_times=dict:store(Grouped, Timestamp, PrevTimes)
      , prev_nums=NewNums
    }
  , NewState
.

%% ----------------------------------------------------------------- %%

-spec do_update(
    Tuples :: [ydb_plan_node:ydb_tuple()]
  , State :: aggr()
    % Either is ?NO_GROUP for no grouping or a tuple which is the key
    % for the synopsis table.
  , Grouped :: ?NO_GROUP | tuple()
) ->
    {NewNum :: dict(), CurrAggr :: term()}
.

%% @doc Performs the update to the synopsis based on the received list
%%      of tuples. Returns the previous and current aggregate.
do_update(
    Tuples
  , #aggr{
        history_size = HistorySize
      , incremental = Incremental
      , schema = Schema
      , columns = Columns
      , result_name = ResultName
      , eval_fun = EvalFun
      , pr_fun = PartialFun
      , aggr_fun = AggrFun
      , synopsis = Synopsis
      , prev_nums = PrevNums
    }
    % GroupKey can either be 'false' indicating no grouping is required,
    % or the key for the group.
  , GroupKey
) when
    is_list(Tuples)
  ->
    % Key to store in the synopsis table.
    SynopsisKey = {ResultName, GroupKey}
  , Partials = get_partials(Synopsis, SynopsisKey)
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

  , NewNum = add_partial(Synopsis, PrevNums, SynopsisKey, NewPartial)

  , case should_evict(Partials, HistorySize) of
        true -> remove_partial(Synopsis, SynopsisKey, erlang:hd(Partials))

      ; false -> pass
    end

    % Technically, for an incremental algorithm, `AggrFun' only needs
    % `NewPartial'.
  , {NewNum, AggrFun(lists:append(Partials, [NewPartial]))}
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

-spec get_partials(
    Synopsis :: ets:tid()
  , SynopsisKey :: {ResultName :: atom(), GroupKey :: ?NO_GROUP | tuple()}
) ->
    Partials :: [term()]
.

%% @doc Returns the list of partial results stored in the synopsis.
get_partials(Synopsis, {ResultName, GroupKey}) ->
    lists:append(ets:match(Synopsis, {{{ResultName, GroupKey}, '_'}, '$1'}))
.

-spec add_partial(
    Synopsis :: ets:tid()
  , Nums :: dict()
  , SynopsisKey :: {ResultName :: atom(), GroupKey :: ?NO_GROUP | tuple()}
  , Partial :: term()
) ->
    NewNums :: dict()
.

%% @doc Inserts the partial result into the synopsis.
add_partial(Synopsis, Nums, SynopsisKey, Partial) ->
    PrevNum = case dict:find(SynopsisKey, Nums) of
        {ok, Value} -> Value

      ; error -> 0
    end

  , CurrNum = PrevNum + 1
  , NewNums = dict:store(SynopsisKey, CurrNum, Nums)
  , ets:insert(Synopsis, {{SynopsisKey, CurrNum}, Partial})
  , NewNums
.

-spec remove_partial(
    Synopsis :: ets:tid()
  , SynopsisKey :: {ResultName :: atom(), GroupKey :: ?NO_GROUP | tuple()}
  , Partial :: term()
) ->
    ok
.

%% @doc Deletes the partial result from the synopsis.
remove_partial(Synopsis, SynopsisKey, Partial) ->
    Num = erlang:hd(lists:append(
        ets:match(Synopsis, {{SynopsisKey, '$1'}, Partial})
    ))

  , ets:delete(Synopsis, {SynopsisKey, Num})
  , ok
.

%% ----------------------------------------------------------------- %%
