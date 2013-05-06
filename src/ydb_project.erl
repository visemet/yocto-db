%% @author Angela Gong <anjoola@anjoola.com>

%% @doc Module for the project operation. Filters a tuple down to
%%      desired columns only.
-module(ydb_project).
-behaviour(ydb_plan_node).

-export([start_link/2, start_link/3]).
-export([init/1, delegate/2, delegate/3, compute_schema/2]).

%% @headerfile "ydb_plan_node.hrl"
-include("ydb_plan_node.hrl").

% Testing for private functions.
-ifdef(TEST).
-export([compute_new_schema/3]).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%% =============================================================== %%%
%%%  internal records and types                                     %%%
%%% =============================================================== %%%

-record(project, {
    columns=[] :: [atom() | {atom(), atom()}]
  , include=true :: boolean()
  , indexes=[] :: [integer()]
  , schema :: dict()
}).

-type project() :: #project{
    columns :: [atom() | {atom(), atom()}]
  , include :: boolean()
  , indexes :: [integer()]
  , schema :: undefined | dict()}.
%% Internal project node state.

-type option() ::
    {columns, Columns :: [
        ColName :: atom()
      | {ColName :: atom(), NewName :: atom()}
    ]}
  | {include, Include :: boolean()}.
%% Options for the project node:
%% <ul>
%%   <li><code>{columns, Columns}</code> - A list of column names to
%%       project the tuples down to. The list contains either an atom
%%       <code>ColName</code> which is the name of the column, or the
%%       tuple <code>{ColName, NewName}</code> which is the current
%%       name of the column and the desired new name.</li>
%%   <li><code>{include, Include}</code> - <code>Include</code> is
%%       <code>true</code> if want to project down to all the columns
%%       specified. Is <code>false</code> if want to project down to
%%       all columns <b>EXCEPT</b> the columns specified. Defaults to
%%       <code>true</code>.</li>
%% </ul>

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-spec start_link(Args :: [option()], Options :: list()) ->
    {ok, Pid :: pid()}
  | ignore
  | {error, Error :: term()}
.

%% @doc Starts the project node in the supervisor hierarchy.
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

%% @doc Starts the project node in the supervisor hierarchy with a
%%      registered name.
start_link(Name, Args, Options) ->
    ydb_plan_node:start_link(Name, ?MODULE, Args, Options)
.

%% ----------------------------------------------------------------- %%

-spec init(Args :: [option()]) ->
    {ok, State :: project()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Initializes the project node's internal state.
init(Args) when is_list(Args) -> init(Args, #project{});

init(_Args) -> {error, {badarg, not_options_list}}.

-spec delegate(Request :: atom(), State :: project()) ->
    {ok, State :: project()}
.

%% @private
%% @doc Passes on the tuple to subscribers after it is projected
%%      down.
delegate(
    _Request = {tuple, Tuple}
  , State = #project{}
) ->
    check_tuple(Tuple, State)
  , {ok, State}
;

delegate(
    _Request = {tuples, Tuples}
  , State = #project{}
) ->
    lists:foreach(fun(Tuple) ->
        check_tuple(Tuple, State) end, Tuples
    )
  , {ok, State}
;

delegate(_Request = {diffs, Tids}, State = #project{}) ->
    {ok, OutTid} = ydb_ets_utils:create_diff_table(project)
  , check_diffs(Tids, State, OutTid)
  , {ok, State}
;

%% @doc Receives the new schema and sets it as part of the state.
delegate(_Request = {schema, Schema}, State = #project{}) ->
    NewState = State#project{schema=dict:from_list(Schema)}
  , {ok, NewState}
;

%% @doc Receives the new set of valid indexes and sets it as part
%%      of the state.
delegate(_Request = {indexes, Indexes}, State = #project{}) ->
    NewState = State#project{indexes=Indexes}
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
  , State :: project()
  , Extras :: list()
) ->
    {ok, NewState :: project()}
.

delegate(_Request, State, _Extras) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec compute_schema(
    InputSchemas :: [ydb_plan_node:ydb_schema()]
  , State :: project()
) ->
    {ok, OutputSchema :: ydb_plan_node:ydb_schema()}
  | {error, {badarg, InputSchemas :: [ydb_plan_node:ydb_schema()]}}
.

%% @doc Returns the output schema of the project node based upon the
%%      supplied input schemas. Expects a single schema.
compute_schema([Schema], #project{columns=Columns, include=Include}) ->
    {Indexes, NewSchema} = compute_new_schema(Schema, Columns, Include)
    
    % Inform self of new schema and list of indexes.
  , ydb_plan_node:relegate(
        erlang:self()
      , {schema, NewSchema}
    )
  , ydb_plan_node:relegate(
        erlang:self()
      , {indexes, Indexes}
    )
  , {ok, NewSchema}
;   

compute_schema(Schemas, #project{}) ->
    {error, {badarg, Schemas}}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: project()) ->
    {ok, State :: project()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Parses initializing arguments to set up the internal state of
%%      the project node.
init([], State = #project{}) ->
    {ok, State}
;

init([{columns, Columns} | Args], State = #project{}) ->
    init(Args, State#project{columns=Columns})
;

init([{include, Include} | Args], State = #project{}) ->
    init(Args, State#project{include=Include})
;

init([Term | _Args], #project{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

-spec compute_new_schema(
    Schema :: ydb_plan_node:ydb_schema()
  , Columns :: [atom() | {atom(), atom()}]
  , Include :: boolean()
) ->
    {Indexes :: [integer()], NewSchema :: ydb_plan_node:ydb_schema()}
.

%% @private
%% @doc Computes the new schema.
compute_new_schema(Schema, Columns, Include) ->
    Indexes = get_indexes(Include, Columns, dict:from_list(Schema))
    % Loop through each index to select out only desired columns to 
    % create a new schema. Also does the renaming if necessary.
  , ColRange = lists:seq(1, length(Indexes))
  , NewSchema = lists:map(fun(I) ->
        Index = lists:nth(I, Indexes)
      , get_col(I, Index, Columns, Schema, Include) end, ColRange
    )
  , {Indexes, NewSchema}
.

%% ----------------------------------------------------------------- %%

-spec project_tuple(
    Tuple :: ydb_plan_node:ydb_tuple()
  , State :: project()
) -> NewTuple :: ydb_plan_node:ydb_tuple().

%% @private
%% @doc Projects the tuple down to the desired columns.
project_tuple(
    Tuple=#ydb_tuple{data=Data}
  , _State=#project{indexes=Indexes}
) ->
    NewData = lists:map(fun(Index) ->
        element(Index, Data) end, Indexes
    )
  , NewTuple = Tuple#ydb_tuple{data=list_to_tuple(NewData)}
  , NewTuple
.

-spec check_tuple(
    Tuple :: ydb_plan_node:ydb_tuple()
  , State :: project()
) -> ok.

%% @private
%% @doc Projects the tuple down to the desired columns, then passes it
%%      along to the project node's listeners.
check_tuple(Tuple, State) ->
    NewTuple = project_tuple(Tuple, State)
  , ydb_plan_node:notify(
        erlang:self()
      , {tuple, NewTuple}
    )
.

-spec check_diffs(
    Tids :: [ets:tid()]
  , State :: project()
  , OutTid :: ets:tid()
) -> ok.

%% @private
%% @doc Projects all the tuples in a diff down to the desired columns,
%%      then passes a new diff along to listeners.
check_diffs(Tids, State, OutTid) ->
    {Ins, Dels} = ydb_ets_utils:extract_diffs(Tids)
    
    % Do the inserts first.
  , PlusDiffs = lists:map(fun(Tuple) ->
        project_tuple(Tuple, State) end, Ins
    )
  , lists:foreach(fun(Tuple) ->
        ydb_ets_utils:add_diffs(OutTid, '+', project, Tuple) end
      , PlusDiffs
    )
    
    % Then do the deletes.
  , MinusDiffs = lists:map(fun(Tuple) ->
        project_tuple(Tuple, State) end, Dels
    )
  , lists:foreach(fun(Tuple) ->
        ydb_ets_utils:add_diffs(OutTid, '-', project, Tuple) end
      , MinusDiffs
    )
.        
    

%% ----------------------------------------------------------------- %%

-spec get_col(
    I :: integer()
  , Index :: integer()
  , Columns :: [atom() | {atom(), atom()}]
  , Schema :: ydb_plan_node:ydb_schema()
  , Include :: boolean()
) ->
    {ColName :: atom(), {I :: integer(), Type :: atom()}}.

    
%% @private
%% @doc Returns the column at a particular index in the schema.
%%      Renames the column if desired.
get_col(I, Index, Columns, Schema, _Include=true) ->
    % Columns is the list of columns we do want, and their renames.
    Column = lists:nth(I, Columns)
  , {_OldName, {_Index, Type}} = lists:nth(Index, Schema)
  , case Column of
        {_OldName, NewName} -> {NewName, {I, Type}}
      ; ColName -> {ColName, {I, Type}}
    end
;

get_col(I, Index, _Columns, Schema, _Include=false) ->
    % Index is the index of the column we DO want from Schema.
    {ColName, {_Index, Type}} = lists:nth(Index, Schema)
  , {ColName, {I, Type}}
.


-spec get_indexes(
    Include :: boolean()
  , Columns :: [atom() | {atom(), atom()}]
  , Schema :: dict())
->
    Indexes :: [integer()]
.

%% @private
%% @doc Gets the list of indexes of the desired columns, based on a list of
%%      column names that are supposed to be excluded or included.
get_indexes(_Include=true, Columns, Schema) ->
    Indexes = lists:map(fun(Col) ->
        get_index(Col, Schema) end, Columns
    )
  , Indexes
;

get_indexes(_Include=false, Columns, Schema) ->
    BadIndexes = get_indexes(true, Columns, Schema)
  , Indexes = lists:seq(1, length(dict:to_list(Schema)))
  , lists:subtract(Indexes, BadIndexes)
.

-spec get_index(
    ColName :: atom() | {atom(), atom()}
  , Schema :: dict())
->
    integer() | error.

%% @doc Gets the index of a particular column.
get_index(_Col={OldName, _NewName}, Schema) ->
    get_index(OldName, Schema)
;

get_index(ColName, Schema) ->
    case dict:find(ColName, Schema) of
        {ok, {Index, _Type}} ->
            Index
      ; error -> error
    end
.

%%% =============================================================== %%%
%%%  private tests                                                  %%%
%%% =============================================================== %%%

-ifdef(TEST).
init_test() ->
    ?assertMatch(
        {ok, #project{columns=[first, second], include=true}}
      , init([], #project{columns=[first, second]})
    )
  , ?assertMatch(
        {ok, #project{include=false}}
      , init([], #project{include=false})
    )
  , ?assertMatch(
        {ok, #project{include=false, columns=[okay]}}
      , init([], #project{include=false, columns=[okay]})
    )
  , ?assertMatch(
        {error, {badarg, bad}}
      , init([bad], #project{})
    )
.
-endif.

%% ----------------------------------------------------------------- %%
