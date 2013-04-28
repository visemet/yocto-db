%% @author Angela Gong <anjoola@anjoola.com>

%% @doc Module for the relation-to-relation MAX aggregate function.
%%      Tracks the max of the values seen so far.
-module(ydb_max_rel).
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

-record(aggr_max, {
    column :: atom() | {atom(), atom()}
  , index :: integer()
  , tid :: ets:tid()
}).

-type aggr_max() :: #aggr_max{
    column :: undefined | atom() | {atom(), atom()}
  , index :: undefined | integer()
  , tid :: undefined | ets:tid()
}.
%% Internal max aggregate state.

-type option() ::
    {column, Column :: atom() | {ColName :: atom(), NewName :: atom()}}
.
%% Options for the MAX aggregate:
%% <ul>
%%   <li><code>{column, Column}</code> - The column name to track the
%%       max of. <code>Column</code> is either an atom
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
    {ok, State :: aggr_max()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Initializes the aggregate node's internal state.
init(Args) when is_list(Args) -> init(Args, #aggr_max{});

init(_Args) -> {error, {badarg, not_options_list}}.

-spec delegate(Request :: atom(), State :: aggr_max()) ->
    {ok, State :: aggr_max()}
.

%% @private
%% @doc Writes the new max to its output table.
delegate(
    _Request = {diffs, Tids}
  , State = #aggr_max{index=Index, tid=SynTid}
) when is_list(Tids) ->
    {ok, OutTid} = ydb_ets_utils:create_table(max)
    
  , [CurrTuple] = ydb_ets_utils:dump_tuples(SynTid, max)
  , {CurrMax} = CurrTuple#ydb_tuple.data
  
    % Update the tuple the output.
  , ydb_ets_utils:add_diffs(OutTid, '-', max, CurrTuple)
  , NewTuple = apply_diffs(Tids, Index, CurrMax, OutTid, SynTid)
  
    % Update the tuple in the synopsis table.
  , ydb_ets_utils:replace_tuple(SynTid, max, CurrTuple, NewTuple)
  , {ok, State}
;

%% @private
%% @doc Receives the valid index and sets it as part of the state.
delegate(_Request = {index, Index}, State = #aggr_max{}) ->
    NewState = State#aggr_max{index=Index}
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
  , State :: aggr_max()
  , Extras :: list()
) ->
    {ok, NewState :: aggr_max()}
.

delegate(_Request, State, _Extras) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec compute_schema(
    InputSchemas :: [ydb_plan_node:ydb_schema()]
  , State :: aggr_max()
) ->
    {ok, OutputSchema :: ydb_plan_node:ydb_schema()}
  | {error, {badarg, InputSchemas :: [ydb_plan_node:ydb_schema()]}}
.

%% @private
%% @doc Returns the output schema of the max aggregate based upon the
%%      supplied input schemas. Expects a single schema.
compute_schema([Schema], #aggr_max{column=Column}) ->
    {Index, NewSchema} =
        ydb_aggr_utils:compute_new_schema(Schema, Column, "MAX")
    % Inform self of index to check for.
  , ydb_plan_node:relegate(
        erlang:self()
      , {index, Index}
    )
  , {ok, NewSchema}
;

compute_schema(Schemas, #aggr_max{}) ->
    {error, {badarg, Schemas}}
.


%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: aggr_max()) ->
    {ok, State :: aggr_max()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Parses initializing arguments to set up the internal state of
%%      the max aggregate node.
init([], State = #aggr_max{}) ->
    post_init(State)
;

init([{column, Column} | Args], State = #aggr_max{}) ->
    init(Args, State#aggr_max{column=Column})
;

init([Term | _Args], #aggr_max{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

%% @private
%% @doc Creates the output table.
post_init(State=#aggr_max{}) ->
    {ok, Tid} = ydb_ets_utils:create_table(max_synopsis)
  , Tuple = #ydb_tuple{timestamp=0, data={undefined}}
  , ydb_ets_utils:add_tuples(Tid, max, Tuple)
  , {ok, State#aggr_max{tid=Tid}}
.

%% ----------------------------------------------------------------- %%

-spec get_max(
    Tuples :: [ydb_plan_node:ydb_tuple()]
  , Index :: integer()
) -> Max :: number().

%% @private
%% @doc Gets the maximum from a list of intermediate maxs stored as
%%      ydb_tuples.
get_max(Tuples, Index) when is_list(Tuples) ->
    MaxList = lists:map(fun(Tuple) ->
        element(Index, Tuple#ydb_tuple.data) end
      , Tuples
    )
  , lists:max(MaxList)
.

%% ----------------------------------------------------------------- %%

-spec apply_diffs(
    Tids :: [ets:tid()]
  , Index :: integer()
  , CurrMax :: number()
  , OutTid :: ets:tid()
  , SynTid :: ets:tid()
) ->
    NewTuple :: ydb_plan_node:ydb_tuple()
.

%% @doc Apply the diffs, update the maximum, and output the results to
%%      the listeners.
apply_diffs(Tids, Index, CurrMax, OutTid, SynTid) ->
    {Ins, Dels} = ydb_ets_utils:extract_diffs(Tids)

    % Apply all the inserts.
  , InterMax = add(CurrMax, Ins, SynTid, Index)
  
    % Apply all the deletes and create a new tuple.
  , NewMax = sub(InterMax, Dels, SynTid, Index)
  , NewTuple = #ydb_tuple{
        data=list_to_tuple([NewMax])
      , timestamp=max(
            ydb_ets_utils:max_timestamp(Ins)
          , ydb_ets_utils:max_timestamp(Dels)
        )
    }
    
    % Add tuple to diffs table.
  , ydb_ets_utils:add_diffs(OutTid, '+', max, NewTuple)
  
    % Send to listeners.
  , ydb_plan_node:notify(
        erlang:self()
      , {diffs, OutTid}
    )
    
    % Return value to update state.
  , NewTuple
.

%% ----------------------------------------------------------------- %%

-spec add(
    CurrMax :: undefined | number()
  , Ins :: [ydb_plan_node:ydb_tuple()]
  , SynTid :: ets:tid()
  , Index :: integer()
) ->
    NewMax :: number().

%% @doc Updates the maximum if a new number is added in.
add(CurrMax, _Ins=[], _SynTid, _Index) ->
    CurrMax
;

add(undefined, Ins, SynTid, Index) ->
    Timestamp = ydb_ets_utils:max_timestamp(Ins)
  , InterMax = get_max(Ins, Index)

    % Add to synopsis table.
  , InterTuple = #ydb_tuple{
        data=list_to_tuple([InterMax])
      , timestamp=Timestamp
    }
  , ydb_ets_utils:add_tuples(SynTid, inter_max, InterTuple)
  , InterMax
;
    
add(CurrMax, Ins, SynTid, Index) ->
    InterMax = add(undefined, Ins, SynTid, Index)
  , max(CurrMax, InterMax)
.

%% ----------------------------------------------------------------- %%

-spec sub(
    InterMax :: number()
  , Dels :: [ydb_plan_node:ydb_tuple()]
  , SynTid :: ets:tid()
  , Index :: integer()
) ->
    NewMax :: number().

%% @doc Updates the maximum if a number is removed.
sub(InterMax, _Dels=[], _SynTid, _Index) ->
    InterMax
;

sub(_InterMax, Dels, SynTid, Index) ->
    % Delete max entry that has fallen out.
    Timestamp = ydb_ets_utils:max_timestamp(Dels)
  , ydb_ets_utils:delete_tuples(SynTid, {inter_max, Timestamp})
  
    % Compute the new max.
  , InterMaxs = ydb_ets_utils:dump_tuples(SynTid, inter_max)
  , NewMax = get_max(InterMaxs, Index)
  , NewMax
.

%%% =============================================================== %%%
%%%  private tests                                                  %%%
%%% =============================================================== %%%

-ifdef(TEST).
init_test() ->
    ?assertMatch(
        {ok, #aggr_max{column=[first]}}
      , init([], #aggr_max{column=[first]})
    )
  , ?assertMatch(
        {error, {badarg, bad}}
      , init([bad], #aggr_max{})
    )
.
-endif.

%% ----------------------------------------------------------------- %%
