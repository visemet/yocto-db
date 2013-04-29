%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc Module for the relation-to-relation SUM aggregate function.
%%      Tracks the sum of the values seen so far.
-module(ydb_sum_rel).
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

-record(aggr_sum, {
    column :: atom() | {atom(), atom()}
  , index :: integer()
  , tid :: ets:tid()
}).

-type aggr_sum() :: #aggr_sum{
    column :: undefined | atom() | {atom(), atom()}
  , index :: undefined | integer()
  , tid :: undefined | ets:tid()
}.
%% Internal sum aggregate state.

-type option() ::
    {column, Column :: atom() | {ColName :: atom(), NewName :: atom()}}
.
%% Options for the SUM aggregate:
%% <ul>
%%   <li><code>{column, Column}</code> - The column name to track the
%%       sum of. <code>Column</code> is either an atom
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
    {ok, State :: aggr_sum()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Initializes the aggregate node's internal state.
init(Args) when is_list(Args) -> init(Args, #aggr_sum{});

init(_Args) -> {error, {badarg, not_options_list}}.

-spec delegate(Request :: atom(), State :: aggr_sum()) ->
    {ok, State :: aggr_sum()}
.

%% @private
%% @doc Writes the new sum to its output table.
delegate(
    _Request = {diffs, Tids}
  , State = #aggr_sum{index=Index, tid=SynTid}
) when is_list(Tids) ->
    {ok, OutTid} = ydb_ets_utils:create_diff_table(sum)

  , [CurrTuple] = ydb_ets_utils:dump_tuples(SynTid)
  , {CurrSum} = CurrTuple#ydb_tuple.data

    % Update the tuple in the output.
  , ydb_ets_utils:add_diffs(OutTid, '-', sum, CurrTuple)
  , NewTuple = apply_diffs(Tids, Index, CurrSum, OutTid)

    % Update the tuple in the synopsis table.
  , ydb_ets_utils:replace_tuple(SynTid, sum, CurrTuple, NewTuple)
  , {ok, State}
;

%% @doc Receives the valid index and sets it as part of the state.
delegate(_Request = {index, Index}, State = #aggr_sum{}) ->
    NewState = State#aggr_sum{index=Index}
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
  , State :: aggr_sum()
  , Extras :: list()
) ->
    {ok, NewState :: aggr_sum()}
.

delegate(_Request, State, _Extras) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec compute_schema(
    InputSchemas :: [ydb_plan_node:ydb_schema()]
  , State :: aggr_sum()
) ->
    {ok, OutputSchema :: ydb_plan_node:ydb_schema()}
  | {error, {badarg, InputSchemas :: [ydb_plan_node:ydb_schema()]}}
.

%% @doc Returns the output schema of the sum aggregate based upon the
%%      supplied input schemas. Expects a single schema.
compute_schema([Schema], #aggr_sum{column=Column}) ->
    {Index, NewSchema} =
        ydb_aggr_utils:compute_new_schema(Schema, Column, "SUM")
    % Inform self of index to check for.
  , ydb_plan_node:relegate(
        erlang:self()
      , {index, Index}
    )
  , {ok, NewSchema}
;

compute_schema(Schemas, #aggr_sum{}) ->
    {error, {badarg, Schemas}}
.


%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: aggr_sum()) ->
    {ok, State :: aggr_sum()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Parses initializing arguments to set up the internal state of
%%      the sum aggregate node.
init([], State = #aggr_sum{}) ->
    post_init(State)
;

init([{column, Column} | Args], State = #aggr_sum{}) ->
    init(Args, State#aggr_sum{column=Column})
;

init([Term | _Args], #aggr_sum{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

%% @private
%% @doc Creates the output table.
post_init(State=#aggr_sum{}) ->
    %gen_server:cast(erlang:self(), {compute_schema})
    {ok, Tid} = ydb_ets_utils:create_table(sum_synopsis)
  , Tuple = #ydb_tuple{timestamp=0, data={0}}
  , ydb_ets_utils:add_tuples(Tid, sum, Tuple)
  , {ok, State#aggr_sum{tid=Tid}}
.

%% ----------------------------------------------------------------- %%

-spec apply_diffs(
    Tids :: [ets:tid()]
  , Index :: integer()
  , CurrSum :: number()
  , OutTid :: ets:tid()
) ->
    NewTuple :: ydb_plan_node:ydb_tuple()
.

%% @doc Apply the diffs, update the sum, and output the results to the
%%      listeners.
apply_diffs(Tids, Index, CurrSum, OutTid) ->
    {Ins, Dels} = ydb_ets_utils:extract_diffs(Tids)

    % Apply all inserts, then deletes.
  , InterSum = lists:foldl(
        fun(X, Curr) -> add(Curr, element(Index, X#ydb_tuple.data)) end
      , CurrSum
      , Ins
    )
  , NewSum = lists:foldl(
        fun(X, Curr) -> sub(Curr, element(Index, X#ydb_tuple.data)) end
      , InterSum
      , Dels
    )

    % Create a new tuple from this sum.
  , NewTuple = #ydb_tuple{
        data=list_to_tuple([NewSum])
      , timestamp=ydb_ets_utils:max_timestamp(Tids, diff)
    }

    % Add tuple to diffs table.
  , ydb_ets_utils:add_diffs(OutTid, '+', sum, NewTuple)

    % Send to listeners.
  , ydb_plan_node:notify(
        erlang:self()
      , {diffs, OutTid}
    )

    % Return value to update state.
  , NewTuple
.

%% ----------------------------------------------------------------- %%

-spec add(Sum :: number(), NewNum :: number()) ->
    NewSum :: number().

%% @doc Gets the sum of two numbers.
add(undefined, NewNum) ->
    NewNum
;

add(Sum, NewNum) when is_number(NewNum) ->
    Sum + NewNum
.

%% ----------------------------------------------------------------- %%

-spec sub(Sum :: number(), NewNum :: number()) ->
    NewSum :: number().

%% @doc Gets the difference of two numbers.
sub(undefined, NewNum) ->
    NewNum
;

sub(Sum, NewNum) when is_number(NewNum) ->
    Sum - NewNum
.

%%% =============================================================== %%%
%%%  private tests                                                  %%%
%%% =============================================================== %%%

-ifdef(TEST).
init_test() ->
    ?assertMatch(
        {ok, #aggr_sum{column=[first]}}
      , init([], #aggr_sum{column=[first]})
    )
  , ?assertMatch(
        {error, {badarg, bad}}
      , init([bad], #aggr_sum{})
    )
.
-endif.

%% ----------------------------------------------------------------- %%
