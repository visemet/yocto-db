%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc Module for the SUM aggregate function. Tracks the sum of the
%%      values seen so far.
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
  , init_tid :: ets:tid()
  , curr_sum=0 :: integer()
}).

-type aggr_sum() :: #aggr_sum{
    column :: undefined | atom() | {atom(), atom()}
  , index :: undefined | integer()
  , tid :: undefined | ets:tid()
  , init_tid :: undefined | ets:tid()
  , curr_sum :: integer()}.
%% Internal sum aggregate state.

-type option() ::
    {column, Column :: atom() | {ColName :: atom(), NewName :: atom()}}
  | {init_tid, Tid :: ets:tid()}
.
%% Options for the SUM aggregate:
%% <ul>
%%   <li><code>{column, Column}</code> - The column name to track the
%%       sum of. <code>Column</code> is either an atom
%%       <code>Column</code> which is the name of the column, or the
%%       tuple <code>{ColName, NewName}</code> which is the current
%%       name of the column and the desired new name.</li>
%%    <li><code>{init_tid, Tid}</code> - The Tid of the table to read from
%%       initially.</li>
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
  , State = #aggr_sum{curr_sum=CurrSum, index=Index, tid=Tid}
) ->
    NewSum = apply_diffs(Tids, Index, CurrSum, Tid)
  , NewState = State#aggr_sum{curr_sum=NewSum}
  , {ok, NewState}
;

%% @doc Reads values from a table and adds them to the sum.
delegate(
    _Request = {read_table, Tid}
  , State = #aggr_sum{curr_sum=CurrSum, index=Index, tid=OutTid}
) ->
    Tuples = ydb_ets_utils:dump_tuples(Tid)
  , NewSum = lists:foldl(
        fun(Tuple, Sum) ->
            check_tuple(Tuple, Index, Sum, OutTid)
        end
      , CurrSum
      , Tuples
    )
  , NewState = State#aggr_sum{curr_sum=NewSum}
  , {ok, NewState}
;

%% @doc Sends output Tid to requesting Pid.
delegate(
    _Request = {get_output, ReqPid}
  , State = #aggr_sum{tid=Tid}
) ->
    ReqPid ! Tid
  , {ok, State}
;

%% @doc Receives the new set of valid indexes and sets it as part
%%      of the state.
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

init([{tid, Tid} | Args], State = #aggr_sum{}) ->
    init(Args, State#aggr_sum{init_tid=Tid})
;

init([Term | _Args], #aggr_sum{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

%% @private
%% @doc Creates the output table.
post_init(State=#aggr_sum{}) ->
    {ok, Tid} = ydb_ets_utils:create_table(sum)
  , {ok, State#aggr_sum{tid=Tid}}
.

%% ----------------------------------------------------------------- %%

-spec check_tuple(
    Tuple :: ydb_plan_node:ydb_tuple()
  , Index :: integer()
  , CurrMin :: number()
  , OutTid :: ets:tid()
) ->
    NewMin :: integer()
.

%% @private
%% @doc Selects only the necessary column required for finding the sum
%%      and adds it to the current sum.
check_tuple(
    Tuple=#ydb_tuple{data=Data}
  , Index
  , CurrSum
  , OutTid
) ->
    RelevantData = element(Index, Data)
  , NewSum = get_sum(CurrSum, RelevantData)
  , NewTuple = Tuple#ydb_tuple{data=list_to_tuple([NewSum])}
  , ydb_ets_utils:add_tuples(OutTid, sum, NewTuple)
  , NewSum
.

%% ----------------------------------------------------------------- %%

-spec get_sum(Sum :: number(), NewNum :: number()) ->
    NewMin :: number()
.

%% @doc Gets the sum of two numbers.
get_sum(undefined, NewNum) ->
    NewNum
;

get_sum(Sum, NewNum) when is_number(NewNum) ->
    NewSum = Sum + NewNum
  , NewSum
.

%% ----------------------------------------------------------------- %%

-spec apply_diffs(
    Tids :: [ets:tid()]
  , Index :: integer()
  , CurrSum :: number()
  , OutTid :: ets:tid()
) ->
    NewSum :: number()
.

%% @doc TODO
apply_diffs(Tids, Index, CurrSum, OutTid) ->
    {Ins, Dels} = ydb_ets_utils:extract_diffs(Tids)

    % apply all inserts, then deletes
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

  , NewTuple = #ydb_tuple{
        data=list_to_tuple([NewSum])
      , timestamp=ydb_ets_utils:max_timestamp(Tids, diff)
    }

  , ydb_ets_utils:add_tuples(OutTid, sum, NewTuple)
  , NewSum
.

%% ----------------------------------------------------------------- %%

-spec add(Sum :: number(), NewNum :: number()) ->
    NewMin :: number().

%% @doc Gets the sum of two numbers.
add(undefined, NewNum) ->
    NewNum
;

add(Sum, NewNum) when is_number(NewNum) ->
    Sum + NewNum
.

-spec sub(Sum :: number(), NewNum :: number()) ->
    NewMin :: number().

%% @doc Gets the sum of two numbers.
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
