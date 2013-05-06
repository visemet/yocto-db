%% @author Angela Gong <anjoola@anjoola.com>

%% @doc Module for the grouping operation. Sorts tuples into groups
%%      and outputs each group together in order.
-module(ydb_group).
-behaviour(ydb_plan_node).

-export([start_link/2, start_link/3]).
-export([init/1, delegate/2, delegate/3, compute_schema/2]).

-export([compute_new_schema/2, sort_tuples/2]).

%% @headerfile "ydb_plan_node.hrl"
-include("ydb_plan_node.hrl").

% Testing for private functions.
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%% =============================================================== %%%
%%%  internal records and types                                     %%%
%%% =============================================================== %%%

-record(group, {
    columns=[] :: [atom()]
  , group_indexes=[] :: [integer()]
  , other_indexes=[] :: [integer()]
  , schema :: dict()
}).

-type group() :: #group{
    columns :: [atom()]
  , group_indexes :: [integer()]
  , other_indexes :: [integer()]
  , schema :: undefined | dict()}.
%% Internal grouping node state.

-type option() ::
    {columns, Columns :: [atom()]}.
%% Options for the grouping node.
%% <ul>
%%   <li><code>{columns, Columns}</code> - Names of the columns to 
%%       group by, in grouping order. <code>Columns</code> should be a
%%       a list of atoms.</li>
%% </ul>

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-spec start_link(Args :: [option()], Options :: list()) ->
    {ok, Pid :: pid()}
  | ignore
  | {error, Error :: term()}
.

%% @doc Starts the select node in the supervisor hierarchy.
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

%% @doc Starts the grouping node in the supervisor hierarchy with a
%%      registered name.
start_link(Name, Args, Options) ->
    ydb_plan_node:start_link(Name, ?MODULE, Args, Options)
.

%% ----------------------------------------------------------------- %%

-spec init(Args :: [option()]) ->
    {ok, State :: group()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Initializes the grouping node's internal state.
init(Args) when is_list(Args) -> init(Args, #group{});

init(_Args) -> {error, {badarg, not_options_list}}.

-spec delegate(Request :: atom(), State :: group()) ->
    {ok, State :: group()}
.

%% @private
%% @doc Passes on the diff to subscribers after it is reordered into
%%      the right groups.
delegate(
    _Request = {diffs, Tids}
  , State = #group{}
) ->
    {ok, OutTid} = ydb_ets_utils:create_diff_table(group)
  , check_diffs(Tids, State, OutTid)
  , {ok, State}
;

%% @doc Receives the new schema and sets it as part of the state.
delegate(_Request = {schema, Schema}, State) ->
    NewState = State#group{schema=dict:from_list(Schema)}
  , {ok, NewState}
;

%% @doc Receives the new set of valid indexes and sets it as part
%%      of the state. Indexes are those on the tuple before it is
%%      reordered.
delegate(
    _Request = {indexes, {GroupIndexes, OtherIndexes}}
  , State=#group{}
) ->
    NewState = State#group{
        group_indexes=GroupIndexes
      , other_indexes=OtherIndexes
    }
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
  , State :: group()
  , Extras :: list()
) ->
    {ok, NewState :: group()}
.

delegate(_Request, State, _Extras) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec compute_schema(
    InputSchemas :: [ydb_plan_node:ydb_schema()]
  , State :: group()
) ->
    {ok, NewSchema :: ydb_plan_node:ydb_schema()}
  | {error, {badarg, InputSchemas :: [ydb_plan_node:ydb_schema()]}}
.

%% @doc Returns the output schema of the group node based upon the
%%      supplied input schemas. Reorders the columns by putting the 
%%      grouped columns first. Expects a single schema.
compute_schema([Schema], #group{columns=Columns}) ->
    {GroupIndexes, OtherIndexes, NewSchema} =
        compute_new_schema(Schema, Columns)
    
    % Inform self of new schema and list of indexes.
  , ydb_plan_node:relegate(
        erlang:self()
      , {schema, Schema}
    )
  , ydb_plan_node:relegate(
        erlang:self()
      , {indexes, {GroupIndexes, OtherIndexes}}
    )
  , {ok, NewSchema}
;

compute_schema(Schemas, #group{}) ->
    {error, {badarg, Schemas}}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: group()) ->
    {ok, State :: group()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Parses initializing arguments to set up the internal state of
%%      the grouping node.
init([], State = #group{}) ->
    {ok, State}
;

init([{columns, Columns} | Args], State = #group{}) ->
    init(Args, State#group{columns=Columns})
;

init([Term | _Args], #group{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

-spec compute_new_schema(
    Schema :: ydb_plan_node:ydb_schema()
  , Columns :: [atom()]
) -> {
    GroupIndexes :: [integer()]
  , OtherIndexes :: [integer()]
  , NewSchema :: ydb_plan_node:ydb_schema()
}.

%% @private
%% @doc Computes the new schema.
compute_new_schema(Schema, Columns) ->
    GroupIndexes = ydb_predicate_utils:get_indexes(
        true
      , Columns
      , dict:from_list(Schema)
    )
    
    % Create a new schema. First have the grouped columns appear.
  , ColRange = lists:seq(1, length(GroupIndexes))
  , InterSchema = lists:map(fun(I) ->
        Index = lists:nth(I, GroupIndexes)
      , ydb_predicate_utils:get_col(I, Index, Columns, Schema, true) end
      , ColRange
    )
    
    % Then have all the other columns appear by adding them to the end
    % of the schema.
  , OtherIndexes = lists:subtract(lists:seq(1, length(Schema)), GroupIndexes)
  , NewRange = lists:seq(1, length(OtherIndexes))
  , NewSchema = InterSchema ++ lists:map(fun(I) ->
        Index = lists:nth(I, OtherIndexes)
      , ydb_predicate_utils:get_col(I + length(ColRange), Index, Schema) end
      , NewRange
    )
  , {GroupIndexes, OtherIndexes, NewSchema}
.

%% ----------------------------------------------------------------- %%

-spec check_diffs(
    Tids :: [ets:tid()]
  , State :: group()
  , OutTid :: ets:tid()
) -> ok.

%% @private
%% @doc Reorders the columns in the tuple and then groups them. Outputs
%%      them in a new diff in lexicographic order.
check_diffs(Tids, State=#group{group_indexes=GroupIndexes}, OutTid) ->
    {Ins, Dels} = ydb_ets_utils:extract_diffs(Tids)
  
    % Do the inserts first.
  , PlusDiffs = lists:map(fun(Tuple) ->
        reorder_tuple(Tuple, State) end, Ins
    )
  , SortedPlusDiffs = sort_tuples(PlusDiffs, length(GroupIndexes))
  , lists:foreach(fun(Tuple) ->
        ydb_ets_utils:add_diffs(OutTid, '+', group, Tuple) end
      , SortedPlusDiffs
    )
  
    % Then do the deletes.
  , MinusDiffs = lists:map(fun(Tuple) ->
        reorder_tuple(Tuple, State) end, Dels
    )
  , SortedMinusDiffs = sort_tuples(MinusDiffs, length(GroupIndexes))
  , lists:foreach(fun(Tuple) ->
        ydb_ets_utils:add_diffs(OutTid, '-', group, Tuple) end
      , SortedMinusDiffs
    )
.

%% ----------------------------------------------------------------- %%

-spec reorder_tuple(
    Tuple :: ydb_plan_node:ydb_tuple()
  , State :: group()
) -> NewTuple :: ydb_plan_node:ydb_tuple().

%% @doc Reorders tuple into the correct order determined by the schema
%%      and grouping specifications.
reorder_tuple(
    Tuple=#ydb_tuple{data=Data}
  , _State=#group{group_indexes=GroupIndexes, other_indexes=OtherIndexes}
) ->
    NewData = lists:map(fun(Index) ->
        element(Index, Data) end, (GroupIndexes ++ OtherIndexes)
    )
  , NewTuple = Tuple#ydb_tuple{data=list_to_tuple(NewData)}
  , NewTuple
.

-spec sort_tuples(
    Tuples :: [ydb_plan_node:ydb_tuple()]
  , NumCols :: integer()
) -> SortedTuples :: [ydb_plan_node:ydb_tuple()].

%% @doc Sorts by the first <tt>NumCols</tt> columns.
% TODO: Does not sort correctly
sort_tuples(Tuples, NumCols) ->
    lists:sort(fun(A, B) ->
        io:format("first: ~w    second: ~w~n~n", [A, B]),
        lists:foldl(fun(Index, Result) ->
            Result and
                (element(Index, A#ydb_tuple.data) =<
                element(Index, B#ydb_tuple.data)) end
          , true
          , lists:seq(1, NumCols)
        ) end
      , Tuples
    )
.
    
%%% =============================================================== %%%
%%%  private tests                                                  %%%
%%% =============================================================== %%%

-ifdef(TEST).
init_test() ->
    ?assertMatch(
        {ok, #group{columns=[first, second]}}
      , init([], #group{columns=[first, second]})
    )
  , ?assertMatch(
        {error, {badarg, bad}}
      , init([bad], #group{})
    )
.
-endif.

%% ----------------------------------------------------------------- %%
