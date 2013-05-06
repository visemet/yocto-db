%% @author Angela Gong <anjoola@anjoola.com>

%% @doc Module for a filter/select operations. Filters tuples based on
%%      a specified predicate.
-module(ydb_select).
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

%% @doc Starts the select node in the supervisor hierarchy with a
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
%% @doc Initializes the select node's internal state.
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

delegate(
    _Request = {diffs, Tids}
  , State = #select{}
) ->
    {ok, OutTid} = ydb_ets_utils:create_diff_table(select)
  , check_diffs(Tids, State, OutTid)
  , {ok, State}
;

delegate(_Request = {schema, Schema}, State) ->
    NewState = State#select{schema=Schema}
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
  , State :: select()
  , Extras :: list()
) ->
    {ok, NewState :: select()}
.

delegate(_Request, State, _Extras) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec compute_schema(
    InputSchemas :: [ydb_plan_node:ydb_schema()]
  , State :: select()
) ->
    {ok, OutputSchema :: ydb_plan_node:ydb_schema()}
  | {error, {badarg, InputSchemas :: [ydb_plan_node:ydb_schema()]}}
.

%% @doc Returns the output schema of the select node based upon the
%%      supplied input schemas. Expects a single schema.
compute_schema([Schema], #select{}) ->
     ydb_plan_node:relegate(
        erlang:self()
      , {schema, Schema}
    )
  , {ok, Schema}
;

compute_schema(Schemas, #select{}) ->
    {error, {badarg, Schemas}}
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
    {ok, State}
;

init([{predicate, Predicate} | Args], State = #select{}) ->
    init(Args, State#select{predicate=Predicate})
;

init([Term | _Args], #select{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

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

-spec check_diffs(
    Tids :: [ets:tid()]
  , State :: select()
  , OutTid :: ets:tid()
) -> ok.

%% @private
%% @doc Checks to see if the tuples in the diff satisfy the predicate.
%%      If so, passes a new diff along to listeners.
check_diffs(Tids, State, OutTid) -> 
    {Ins, Dels} = ydb_ets_utils:extract_diffs(Tids)
 
    % Do the inserts first.
  , lists:foreach(fun(Tuple) ->
        check_diff(Tuple, State, '+', OutTid) end, Ins
    )
    % Then do the deletes.
  , lists:foreach(fun(Tuple) ->
        check_diff(Tuple, State, '-', OutTid) end, Dels
    )
.
    
-spec check_diff(
    Tuple :: ydb_plan_node:ydb_tuple()
  , State :: #select{}
  , Op :: atom()
  , OutTid :: ets:tid()
) -> ok.

%% @private
%% @doc Checks a single tuple and if it satisfies the predicate, adds
%%      it to the output Tid.
check_diff(
    Tuple
  , _State=#select{predicate=Predicate, schema=Schema}
  , Op
  , OutTid
) ->
    IsSatisfied = ydb_predicate_utils:is_satisfied(Tuple, Schema, Predicate)
  , if IsSatisfied ->
        ydb_ets_utils:add_diffs(OutTid, Op, select, Tuple)
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
