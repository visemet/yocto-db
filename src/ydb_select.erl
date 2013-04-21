%% @author Angela Gong <anjoola@anjoola.com>

%% @doc Module for a filter/select operations. Filters tuples based on
%%      a specified predicate.
-module(ydb_select).
-behaviour(ydb_plan_node).

-export([start_link/2, start_link/3]).
-export([init/1, delegate/2, delegate/3]).

%% @headerfile "ydb_plan_node.hrl"
-include("ydb_plan_node.hrl").

% Testing for private functions.
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%% =============================================================== %%%
%%%  internal records and types                                     %%%
%%% =============================================================== %%%

-record(select, {
    predicate :: ydb_plan_node:ydb_clause()
  , schema :: ydb_plan_node:ydb_schema()
}).

-type select() :: #select{
    predicate :: undefined | ydb_plan_node:ydb_clause()
  , schema :: undefined | ydb_plan_node:ydb_schema()}.
%% Internal select node state.

-type option() ::
    {predicate, Predicate :: ydb_plan_node:ydb_clause()}.
%% Options for the select node:
%% <ul>
%%   <li><code>{predicate, Predicate}</code> - Filters tuples based on
%%       a predicate <code>Predicate</code>.</li>
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
    {ok, State :: select()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Initializes the input node's internal state.
init(Args) when is_list(Args) -> init(Args, #select{});

init(_Args) -> {error, {badarg, not_options_list}}.

-spec delegate(Request :: atom(), State :: select()) ->
    {ok, State :: select()}
.

%% @private
%% @doc Passes on the tuple to subscribers if it is satisfied by the
%%      given predicate.
delegate(
    _Request = {tuple, Tuple}
  , State = #select{}
) ->
    check_tuple(Tuple, State)
  , {ok, State}
;

delegate(
    _Request = {tuples, Tuples}
  , State = #select{}
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
  , State :: select()
  , Extras :: list()
) ->
    {ok, NewState :: select()}
.

%% @private
%% @doc Receives the schema of the tuples and adds it to the state.
delegate(_Request = {get_schema}, State, _Extras = [Schema, _Timestamp]) ->
    NewState = State#select{schema=Schema}
  , {ok, NewState}
;

delegate(_Request, State, _Extras) ->
    {ok, State}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: select()) ->
    {ok, State :: select()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Parses initializing arguments to set up the internal state of
%%      the select node.
init([], State = #select{}) ->
    post_init(State)
;

init([{predicate, Predicate} | Args], State = #select{}) ->
    init(Args, State#select{predicate=Predicate})
;

init([Term | _Args], #select{}) ->
    {error, {badarg, Term}}
.

-spec post_init(State :: select()) -> {ok, NewState :: select()}.

%% @private
%% @doc Gets the schema for the tuples.
post_init(State = #select{}) ->
    ydb_plan_node:relegate(
        erlang:self()
      , {get_schema}
      , [schema, timestamp]
    )
  , {ok, State}
.

-spec check_tuple(
    Tuple :: ydb_plan_node:ydb_tuple()
  , State :: select()
) -> ok.

%% @private
%% Checks to see if this tuple satisfies the predicate. If so,
%% passes the tuple on to the select node's listeners.
check_tuple(Tuple, _State=#select{predicate=Predicate, schema=Schema}) ->
    IsSatisfied = ydb_predicate_utils:is_satisfied(Tuple, Schema, Predicate)
  , if IsSatisfied ->
        ydb_plan_node:notify(
            erlang:self()
          , {tuple, Tuple}
        )
      ; true -> ok
    end
.

%%% =============================================================== %%%
%%%  private tests                                                  %%%
%%% =============================================================== %%%

-ifdef(TEST).
init_test() ->
    ?assertMatch(
        {ok, #select{predicate={salary, 'lte', tips}}}
      , init([], #select{predicate={salary, 'lte', tips}})
    )
  , ?assertMatch(
        {error, {badarg, bad}}
      , init([bad], #select{})
    )
.
-endif.

%% ----------------------------------------------------------------- %%
