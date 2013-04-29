%% @author Angela Gong <anjoola@anjoola.com>

%% @doc Module for the relation-to-relation AVG aggregate function.
%%      Tracks the average of the values seen so far.
-module(ydb_avg_rel).
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

-record(aggr_avg, {
    column :: atom() | {atom(), atom()}
  , index :: integer()
  , tid :: ets:tid()
}).

-type aggr_avg() :: #aggr_avg{
    column :: undefined | atom() | {atom(), atom()}
  , index :: undefined | integer()
  , tid :: undefined | ets:tid()
}.
%% Internal avg aggregate state.

-type option() ::
    {column, Column :: atom() | {ColName :: atom(), NewName :: atom()}}
.
%% Options for the AVG aggregate:
%% <ul>
%%   <li><code>{column, Column}</code> - The column name to track the
%%       average of. <code>Column</code> is either an atom
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
    {ok, State :: aggr_avg()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Initializes the aggregate node's internal state.
init(Args) when is_list(Args) -> init(Args, #aggr_avg{});

init(_Args) -> {error, {badarg, not_options_list}}.

-spec delegate(Request :: atom(), State :: aggr_avg()) ->
    {ok, State :: aggr_avg()}
.

%% @private
%% @doc Writes the new average to its output table.
delegate(
    _Request = {diffs, Tids}
  , State = #aggr_avg{index=Index, tid=SynTid}
) when is_list(Tids) ->
    {ok, OutTid} = ydb_ets_utils:create_diff_table(avg)

  , [CurrTuple] = ydb_ets_utils:dump_tuples(SynTid)
  , {CurrCount, CurrSum} = CurrTuple#ydb_tuple.data

    % Update the tuple in the output.
  , ydb_ets_utils:add_diffs(OutTid, '-', avg, CurrTuple)
  , NewTuple = apply_diffs(Tids, Index, {CurrCount, CurrSum}, OutTid)

    % Update the tuple in the synopsis table.
  , ydb_ets_utils:replace_tuple(SynTid, avg, CurrTuple, NewTuple)
  , {ok, State}
;

%% @doc Receives the valid index and sets it as part of the state.
delegate(_Request = {index, Index}, State = #aggr_avg{}) ->
    NewState = State#aggr_avg{index=Index}
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
  , State :: aggr_avg()
  , Extras :: list()
) ->
    {ok, NewState :: aggr_avg()}
.

delegate(_Request, State, _Extras) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec compute_schema(
    InputSchemas :: [ydb_plan_node:ydb_schema()]
  , State :: aggr_avg()
) ->
    {ok, OutputSchema :: ydb_plan_node:ydb_schema()}
  | {error, {badarg, InputSchemas :: [ydb_plan_node:ydb_schema()]}}
.

%% @doc Returns the output schema of the avg aggregate based upon the
%%      supplied input schemas. Expects a single schema.
compute_schema([Schema], #aggr_avg{column=Column}) ->
    {Index, NewSchema} =
        ydb_aggr_utils:compute_new_schema(Schema, Column, "AVG")
    % Inform self of index to check for.
  , ydb_plan_node:relegate(
        erlang:self()
      , {index, Index}
    )
  , {ok, NewSchema}
;

compute_schema(Schemas, #aggr_avg{}) ->
    {error, {badarg, Schemas}}
.


%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: aggr_avg()) ->
    {ok, State :: aggr_avg()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Parses initializing arguments to set up the internal state of
%%      the avg aggregate node.
init([], State = #aggr_avg{}) ->
    post_init(State)
;

init([{column, Column} | Args], State = #aggr_avg{}) ->
    init(Args, State#aggr_avg{column=Column})
;

init([Term | _Args], #aggr_avg{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

%% @private
%% @doc Creates the output table.
post_init(State=#aggr_avg{}) ->
    {ok, Tid} = ydb_ets_utils:create_table(avg_synopsis)
  , Tuple = #ydb_tuple{timestamp=0, data={0, 0}}
  , ydb_ets_utils:add_tuples(Tid, avg, Tuple)
  , {ok, State#aggr_avg{tid=Tid}}
.

%% ----------------------------------------------------------------- %%

-spec apply_diffs(
    Tids :: [ets:tid()]
  , Index :: integer()
  , CurrAvg :: {CurrCount :: integer(), CurrSum :: number()}
  , OutTid :: ets:tid()
) ->
    NewTuple :: ydb_plan_node:ydb_tuple()
.

%% @doc Apply the diffs, update the average, and output the results
%%      to the listeners.
apply_diffs(Tids, Index, {CurrCount, CurrSum}, OutTid) ->
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
  , NewCount = CurrCount + length(Ins) - length(Dels)
  , Avg = NewSum / NewCount

    % Create a new tuple from this average.
  , NewTuple = #ydb_tuple{
        data=list_to_tuple([Avg])
      , timestamp=ydb_ets_utils:max_timestamp(Tids, diff)
    }

    % Add tuple to diffs table.
  , ydb_ets_utils:add_diffs(OutTid, '+', avg, NewTuple)

    % Send to listeners.
  , ydb_plan_node:notify(
        erlang:self()
      , {diffs, OutTid}
    )

    % Return value to update state.
  , Tuple = NewTuple#ydb_tuple{data=list_to_tuple([NewCount, NewSum])}
  , Tuple
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
        {ok, #aggr_avg{column=[first]}}
      , init([], #aggr_avg{column=[first]})
    )
  , ?assertMatch(
        {error, {badarg, bad}}
      , init([bad], #aggr_avg{})
    )
.
-endif.

%% ----------------------------------------------------------------- %%
