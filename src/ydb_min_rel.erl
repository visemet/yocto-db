%% @author Angela Gong <anjoola@anjoola.com>

%% @doc Module for the relation-to-relation MIN aggregate function.
%%      Tracks the min of the values seen so far.
-module(ydb_min_rel).
-behaviour(ydb_plan_node).

-export([start_link/2, start_link/3]).
-export([init/1, delegate/2, delegate/3, compute_schema/2]).

-export([add/2]).

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
  , tid :: undefined | ets:tid()
}.
%% Internal min aggregate state.

-type option() ::
    {column, Column :: atom() | {ColName :: atom(), NewName :: atom()}}
.
%% Options for the MIN aggregate:
%% <ul>
%%   <li><code>{column, Column}</code> - The column name to track the
%%       min of. <code>Column</code> is either an atom
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
    {ok, State :: aggr_min()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Initializes the aggregate node's internal state.
init(Args) when is_list(Args) -> init(Args, #aggr_min{});

init(_Args) -> {error, {badarg, not_options_list}}.

-spec delegate(Request :: atom(), State :: aggr_min()) ->
    {ok, State :: aggr_min()}
.

%% @private
%% @doc Writes the new min to its output table.
delegate(
    _Request = {diffs, Tids}
  , State = #aggr_min{index=Index, tid=SynTid}
) when is_list(Tids) ->
    {ok, OutTid} = ydb_ets_utils:create_table(min)
    
  , [CurrTuple] = ydb_ets_utils:dump_tuples(SynTid, min)
  , {CurrMin} = CurrTuple#ydb_tuple.data
  
  , ydb_ets_utils:add_diffs(OutTid, '-', min, CurrTuple)
  , NewTuple = apply_diffs(Tids, Index, CurrMin, OutTid, SynTid)
  
  , ydb_ets_utils:replace_tuple(SynTid, min, CurrTuple, NewTuple)
  , {ok, State}
;

%% @private
%% @doc Receives the valid index and sets it as part of the state.
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

%% @private
%% @doc Returns the output schema of the min aggregate based upon the
%%      supplied input schemas. Expects a single schema.
compute_schema([Schema], #aggr_min{column=Column}) ->
    {Index, NewSchema} =
        ydb_aggr_utils:compute_new_schema(Schema, Column, "MIN")
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
%%      the min aggregate node.
init([], State = #aggr_min{}) ->
    post_init(State)
;

init([{column, Column} | Args], State = #aggr_min{}) ->
    init(Args, State#aggr_min{column=Column})
;

init([Term | _Args], #aggr_min{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

%% @private
%% @doc Creates the output tables.
post_init(State=#aggr_min{}) ->
    {ok, Tid} = ydb_ets_utils:create_table(min_synopsis)
  , Tuple = #ydb_tuple{timestamp=0, data={null}}
  , ydb_ets_utils:add_tuples(Tid, min, Tuple)
  , {ok, State#aggr_min{tid=Tid}}
.

%% ----------------------------------------------------------------- %%

-spec get_mins(Tuples :: [ydb_plan_node:ydb_tuple()]) -> Min :: number().

%% @private
%% @doc Gets the minimum from a list of intermediate mins stored as
%%      ydb_tuples.
get_mins(Tuples) when is_list(Tuples) ->
    MinList = lists:map(fun(Tuple) ->
        element(1, Tuple#ydb_tuple.data) end
      , Tuples
    )
  , lists:min(MinList)
.

%% ----------------------------------------------------------------- %%

-spec apply_diffs(
    Tids :: [ets:tid()]
  , Index :: integer()
  , CurrMin :: number()
  , OutTid :: ets:tid()
  , SynTid :: ets:tid()
) ->
    NewTuple :: ydb_plan_node:ydb_tuple()
.

%% @doc Apply the diffs, update the minimum, and output the results to
%%      the listeners.
apply_diffs(Tids, Index, CurrMin, OutTid, SynTid) ->
    {Ins, Dels} = ydb_ets_utils:extract_diffs(Tids)

    % Apply all the inserts.
  , InterMin = lists:foldl(
        fun(X, Curr) -> add(Curr, element(Index, X#ydb_tuple.data)) end
      , CurrMin
      , Ins
    )
    % Store intermediate min into ets table.
  , Timestamp = ydb_ets_utils:max_timestamp(Tids, diff)
  , InterTuple = #ydb_tuple{
        data=list_to_tuple([InterMin])
      , timestamp=Timestamp
    }
  , ydb_ets_utils:add_tuples(SynTid, inter_min, InterTuple)
  
    % Apply all the deletes.
  , NewMin = sub(Dels, SynTid)
  , NewTuple = #ydb_tuple{
        data=list_to_tuple([NewMin])
      , timestamp=Timestamp
    }
    
    % Add tuple to diffs table.
  , ydb_ets_utils:add_diffs(OutTid, '+', min, NewTuple)
  
    % Send to listeners.
  , ydb_plan_node:notify(
        erlang:self()
      , {diffs, OutTid}
    )
    
    % Return value to update state.
  , NewTuple
.

%% ----------------------------------------------------------------- %%

-spec add(Min :: number(), NewNum :: number()) ->
    NewMin :: number().

%% @doc Updates the minimum if a new number is added in.
add(undefined, NewNum) ->
    NewNum
;

add(Min, NewNum) when is_number(NewNum) ->
    min(Min, NewNum)
.

%% ----------------------------------------------------------------- %%

-spec sub(
    Dels :: [ydb_plan_node:ydb_tuple()]
  , SynTid :: ets:tid()
) ->
    NewMin :: number().

%% @doc Updates the minimum if a number is removed.
sub(Dels, SynTid) ->
    % Delete min entry that has fallen out.
    Timestamp = ydb_ets_utils:max_timestamp(Dels)
  , ydb_ets_utils:delete_tuples(SynTid, {inter_min, Timestamp})
  
    % Compute the new min.
  , InterMins = ydb_ets_utils:dump_tuples(SynTid, inter_min)
  , NewMin = get_mins(InterMins)
  , NewMin
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
