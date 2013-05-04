%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc Module for the relation-to-relation VAR aggregate function.
%%      Tracks the average of the values seen so far.
-module(ydb_var_rel).
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

-record(aggr_var, {
    column :: atom() | {atom(), atom()}
  , index :: integer()
  , tid :: ets:tid()
}).

-type aggr_var() :: #aggr_var{
    column :: undefined | atom() | {atom(), atom()}
  , index :: undefined | integer()
  , tid :: undefined | ets:tid()
}.
%% Internal var aggregate state.

-type option() ::
    {column, Column :: atom() | {ColName :: atom(), NewName :: atom()}}
.
%% Options for the VAR aggregate:
%% <ul>
%%   <li><code>{column, Column}</code> - The column name to track the
%%       variance of. <code>Column</code> is either an atom
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
    {ok, State :: aggr_var()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Initializes the aggregate node's internal state.
init(Args) when is_list(Args) -> init(Args, #aggr_var{});

init(_Args) -> {error, {badarg, not_options_list}}.

-spec delegate(Request :: atom(), State :: aggr_var()) ->
    {ok, State :: aggr_var()}
.

%% @private
%% @doc Writes the new average to its output table.
delegate(
    _Request = {diffs, Tids}
  , State = #aggr_var{index=Index, tid=SynTid}
) when is_list(Tids) ->
    {ok, OutTid} = ydb_ets_utils:create_diff_table(var)

  , [CurrTuple] = ydb_ets_utils:dump_tuples(SynTid)
  , {CurrCount, CurrSum, CurrPSum} = CurrTuple#ydb_tuple.data

    % Update the tuple in the output.
  , CurrVar = compute_var(CurrCount, CurrSum, CurrPSum)
  , ydb_ets_utils:add_diffs(
        OutTid, '-', var, CurrTuple#ydb_tuple{data={CurrVar}})
  , NewTuple = apply_diffs(Tids, Index, {CurrCount, CurrSum, CurrPSum}, OutTid)

    % Update the tuple in the synopsis table.
  , ydb_ets_utils:replace_tuple(SynTid, var, CurrTuple, NewTuple)
  , {ok, State}
;

%% @doc Receives the valid index and sets it as part of the state.
delegate(_Request = {index, Index}, State = #aggr_var{}) ->
    NewState = State#aggr_var{index=Index}
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
  , State :: aggr_var()
  , Extras :: list()
) ->
    {ok, NewState :: aggr_var()}
.

delegate(_Request, State, _Extras) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec compute_schema(
    InputSchemas :: [ydb_plan_node:ydb_schema()]
  , State :: aggr_var()
) ->
    {ok, OutputSchema :: ydb_plan_node:ydb_schema()}
  | {error, {badarg, InputSchemas :: [ydb_plan_node:ydb_schema()]}}
.

%% @doc Returns the output schema of the var aggregate based upon the
%%      supplied input schemas. Expects a single schema.
compute_schema([Schema], #aggr_var{column=Column}) ->
    {Index, NewSchema} =
        ydb_aggr_utils:compute_new_schema(Schema, Column, "VAR")
    % Inform self of index to check for.
  , ydb_plan_node:relegate(
        erlang:self()
      , {index, Index}
    )
  , {ok, NewSchema}
;

compute_schema(Schemas, #aggr_var{}) ->
    {error, {badarg, Schemas}}
.


%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: aggr_var()) ->
    {ok, State :: aggr_var()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Parses initializing arguments to set up the internal state of
%%      the var aggregate node.
init([], State = #aggr_var{}) ->
    post_init(State)
;

init([{column, Column} | Args], State = #aggr_var{}) ->
    init(Args, State#aggr_var{column=Column})
;

init([Term | _Args], #aggr_var{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

%% @private
%% @doc Creates the output table.
post_init(State=#aggr_var{}) ->
    {ok, Tid} = ydb_ets_utils:create_table(var_synopsis)
  , Tuple = #ydb_tuple{timestamp=0, data={0, 0, 0}}
  , ydb_ets_utils:add_tuples(Tid, var, Tuple)
  , {ok, State#aggr_var{tid=Tid}}
.

%% ----------------------------------------------------------------- %%

-spec apply_diffs(
    Tids :: [ets:tid()]
  , Index :: integer()
  , CurrVar :: {
        CurrCount :: integer()
      , CurrSum :: number()
      , CurrPSum :: number()}
  , OutTid :: ets:tid()
) ->
    NewTuple :: ydb_plan_node:ydb_tuple()
.

%% @doc Apply the diffs, update the average, and output the results
%%      to the listeners.
apply_diffs(Tids, Index, {CurrCount, CurrSum, CurrPSum}, OutTid) ->
    {Ins, Dels} = ydb_ets_utils:extract_diffs(Tids)

    % Apply all inserts, then deletes.
  , {InterSum, InterPSum} = lists:foldl(
        fun(X, Curr) -> add(Curr, element(Index, X#ydb_tuple.data)) end
      , {CurrSum, CurrPSum}
      , Ins
    )
  , {NewSum, NewPSum} = lists:foldl(
        fun(X, Curr) -> sub(Curr, element(Index, X#ydb_tuple.data)) end
      , {InterSum, InterPSum}
      , Dels
    )
  , NewCount = CurrCount + length(Ins) - length(Dels)
  , Var = compute_var(NewCount, NewSum, NewPSum)

    % Create a new tuple from this average.
  , NewTuple = #ydb_tuple{
        data=list_to_tuple([Var])
      , timestamp=ydb_ets_utils:max_timestamp(Tids, diff)
    }

    % Add tuple to diffs table.
  , ydb_ets_utils:add_diffs(OutTid, '+', var, NewTuple)

    % Send to listeners.
  , ydb_plan_node:notify(
        erlang:self()
      , {diffs, [OutTid]}
    )

    % Return value to update state.
  , Tuple = NewTuple#ydb_tuple{data=list_to_tuple([NewCount, NewSum, NewPSum])}
  , Tuple
.

%% ----------------------------------------------------------------- %%

-spec add(
    {Sum :: undefined | number(), PSum :: undefined | number()}
  , NewNum :: number()
) ->
    {NewSum :: number(), NewPSum :: number()}
.

%% @doc Gets the sum and psum of two numbers.

add({undefined, undefined}, NewNum) when is_number(NewNum) ->
    {NewNum, NewNum * NewNum}
;

add({Sum, PSum}, NewNum) when is_number(NewNum) ->
    {Sum + NewNum, PSum + NewNum * NewNum}
.

%% ----------------------------------------------------------------- %%

-spec sub(
    {Sum :: undefined | number(), PSum :: undefined | number()}
  , NewNum :: number()
) ->
    {NewSum :: number(), NewPSum :: number()}
.

%% @doc Gets the difference of two numbers.
sub({undefined, undefined}, NewNum) when is_number(NewNum) ->
    {-1 * NewNum, -1 * NewNum * NewNum}
;

sub({Sum, PSum}, NewNum) when is_number(NewNum) ->
    {Sum - NewNum, PSum - NewNum * NewNum}
.

%% ----------------------------------------------------------------- %%

-spec compute_var(Count :: integer(), Sum :: number(), PSum :: number()) ->
    Variance :: number().

%% @doc Computes the variance given the count, sum, and psum. If the count
%%      is 0, returns 0.
compute_var(0, _, _) ->
    0
;
compute_var(Count, Sum, PSum) ->
    (Count * PSum - Sum * Sum)/(Count * Count)
.

%%% =============================================================== %%%
%%%  private tests                                                  %%%
%%% =============================================================== %%%

-ifdef(TEST).
init_test() ->
    ?assertMatch(
        {ok, #aggr_var{column=[first]}}
      , init([], #aggr_var{column=[first]})
    )
  , ?assertMatch(
        {error, {badarg, bad}}
      , init([bad], #aggr_var{})
    )
.
-endif.

%% ----------------------------------------------------------------- %%
