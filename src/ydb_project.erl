%% @author Angela Gong <anjoola@anjoola.com>

%% @doc Module for the project operation. Filters a tuple down to
%%      desired columns only.
-module(ydb_project).
-behaviour(ydb_plan_node).

-export([start_link/2, start_link/3]).
-export([init/1, delegate/2, delegate/3]).

-include_lib("ydb_plan_node.hrl").

% Testing for private functions.
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

% TODO remove
-export([get_index/2]).

%%% =============================================================== %%%
%%%  internal records and types                                     %%%
%%% =============================================================== %%%

-record(project, {
    columns :: [atom() | {atom(), atom()}]
  , include=true :: boolean()
  , schema :: dict()
}).

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
    {ok, State :: #project{}}
  | {error, {badarg, Term :: term}}
.

%% @private
%% @doc Initializes the input node's internal state.
init(Args) when is_list(Args) -> init(Args, #project{}).

-spec delegate(Request :: atom(), State :: #project{}) ->
    {ok, State :: #project{}}
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

delegate(_Request = {info, Message}, State) ->
    delegate(Message, State)
;

delegate(_Request, State) ->
    {ok, State}
.

-spec delegate(
    Request :: atom()
  , State :: #project{}
  , Extras :: list()
) ->
    {ok, NewState :: #project{}}
.

%% @private
%% @doc Receives the schema of the tuples and adds it to the state.
delegate(_Request = {get_schema}, State, _Extras = [Schema, _Timestamp]) ->
    NewState = State#project{schema=dict:from_list(Schema)}
  , {ok, NewState}
;

delegate(_Request, State, _Extras) ->
    {ok, State}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: #project{}) ->
    {ok, State :: #project{}}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Parses initializing arguments to set up the internal state of
%%      the project node.
init([], State = #project{}) ->
    post_init(State)
;

init([{columns, Columns} | Args], State = #project{}) ->
    init(Args, State#project{columns=Columns})
;

init([Term | _Args], #project{}) ->
    {error, {badarg, Term}}
.

-spec post_init(State :: #project{}) -> {ok, NewState :: #project{}}.

%% @private
%% @doc Gets the schema for the tuples.
post_init(State = #project{}) ->
    ydb_plan_node:relegate(
        erlang:self()
      , {get_schema}
      , [schema, timestamp]
    )
  , {ok, State}
.

-spec check_tuple(
    Tuple :: #ydb_tuple{}
  , State :: #project{}
) -> ok.

%% @private
%% Projects the tuple down to the desired columns, then passes it along
%% to the project node's listeners.
check_tuple(
    Tuple=#ydb_tuple{data=Data}
  , _State=#project{columns=Columns, schema=Schema}
) ->
    NewData = lists:map(fun(Col) ->
        element(get_index(Col, Schema), Data) end, Columns
    )
  , NewTuple = Tuple#ydb_tuple{data=list_to_tuple(NewData)}
  , ydb_plan_node:notify(
        erlang:self()
      , {tuple, NewTuple}
    )
.
% TODO handle timestamps

-spec get_index(Column :: atom() | {atom(), atom()}, Schema :: dict()) ->
    Index :: integer()
.

%% @private
%% @doc Gets the index of the column based on the schema.
get_index({ColName, _NewName}, Schema) ->
    get_index(ColName, Schema)
    % TODO change schema based on renames
  % TODO handle unknown names
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
