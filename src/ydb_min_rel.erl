%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc Module for the MIN aggregate function. Tracks the min of the
%%      values seen so far.
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
  , init_tid :: ets:tid()
  , curr_min :: integer()
}).

-type aggr_min() :: #aggr_min{
    column :: undefined | atom() | {atom(), atom()}
  , index :: undefined | integer()
  , tid :: undefined | ets:tid()
  , init_tid :: undefined | ets:tid()
  , curr_min :: undefined | integer()}.
%% Internal min aggregate state.

-type option() ::
    {column, Column :: atom() | {ColName :: atom(), NewName :: atom()}}
  | {init_tid, Tid :: ets:tid()}
.
%% Options for the MIN aggregate:
%% <ul>
%%   <li><code>{column, Column}</code> - The column name to track the
%%       min of. <code>Column</code> is either an atom
%%       <code>Column</code> which is the name of the column, or the
%%       tuple <code>{ColName, NewName}</code> which is the current
%%       name of the column and the desired new name.</li>
%%    <li><code>{init_tid, Tid}</code> - The Tid of the table to read
%%       from initially.</li>
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
  , State = #aggr_min{
        curr_min=CurrMin
      , index=Index
      , tid=Tid
    }
) ->
    NewMin = apply_diffs(Tids, Index, CurrMin, Tid)
  , NewState = State#aggr_min{curr_min=NewMin}
  , {ok, NewState}
;

%% @doc Reads values from a table and adds them to the min.
delegate(
    _Request = {read_table, Tid}
  , State = #aggr_min{curr_min=CurrMin, index=Index, tid=OutTid}
) ->
    Tuples = ydb_ets_utils:dump_tuples(Tid)
  , NewMin = lists:foldl(
        fun(Tuple, Min) ->
            check_tuple(Tuple, Index, Min, OutTid)
        end
      , CurrMin
      , Tuples
    )
  , NewState = State#aggr_min{curr_min=NewMin}
  , {ok, NewState}
;

%% @doc Sends output Tid to requesting Pid.
delegate(
    _Request = {get_output, ReqPid}
  , State = #aggr_min{tid=Tid}
) ->
    ReqPid ! Tid
  , {ok, State}
;

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

init([{tid, Tid} | Args], State = #aggr_min{}) ->
    init(Args, State#aggr_min{init_tid=Tid})
;

init([Term | _Args], #aggr_min{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

%% @private
%% @doc Creates the output tables.
post_init(State=#aggr_min{}) ->
    {ok, Tid} = ydb_ets_utils:create_table(min)
  , {ok, State#aggr_min{tid=Tid}}
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
%% @doc Selects only the necessary column required for finding the min
%%      and updates the current min.
check_tuple(
    Tuple=#ydb_tuple{data=Data}
  , Index
  , CurrMin
  , OutTid
) ->
    RelevantData = element(Index, Data)
  , NewMin = get_min(CurrMin, RelevantData)
  , NewTuple = Tuple#ydb_tuple{data=list_to_tuple([NewMin])}
  , ydb_ets_utils:add_tuples(OutTid, min, NewTuple)
  , NewMin
.

%% ----------------------------------------------------------------- %%

-spec get_min(Min :: number(), NewNum :: number()) ->
    NewMin :: number()
.

%% @doc Gets the min of two numbers.
get_min(undefined, NewNum) ->
    NewNum
;

get_min(Min, NewNum) when is_number(NewNum) ->
    NewMin = min(Min, NewNum)
  , NewMin
.

-spec get_mins(Tuples :: [ydb_plan_node:ydb_tuple()]) -> Min :: number().

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
) ->
    NewMin :: number()
.

%% @doc TODO
apply_diffs(Tids, Index, CurrMin, OutTid) ->
    {Ins, Dels} = ydb_ets_utils:extract_diffs(Tids)
  , Timestamp = ydb_ets_utils:max_timestamp(Tids, diff)

    % Apply all the inserts, then deletes.
  , InterMin = lists:foldl(
        fun(X, Curr) -> add(Curr, element(Index, X#ydb_tuple.data)) end
      , CurrMin
      , Ins
    )
  , io:format("HERE ~w~n~n~n", [InterMin])  
    
    % Store intermediate min into ets table.
  , InterTuple = #ydb_tuple{
        data=list_to_tuple([InterMin])
      , timestamp=Timestamp
    }
  , ydb_ets_utils:add_tuples(OutTid, inter_min, InterTuple)
  
  , NewMin = sub(InterMin, Dels, OutTid)
  , NewTuple = #ydb_tuple{
        data=list_to_tuple([NewMin])
      , timestamp=Timestamp
    }
  , ydb_ets_utils:add_tuples(OutTid, min, NewTuple)
  , NewMin
.

%% ----------------------------------------------------------------- %%

-spec add(Min :: number(), NewNum :: number()) ->
    NewMin :: number().

%% @doc Updates the minimum if a new number is added in.
add(undefined, NewNum) ->
io:format("GOOD", []),
    NewNum
;

add(Min, NewNum) when is_number(NewNum) ->
   io:format("WHAT", []),
    min(Min, NewNum)
.

-spec sub(
    Min :: number()
  , Dels :: [ydb_plan_node:ydb_tuple()]
  , Tid :: ets:tid()
) ->
    NewMin :: number().

%% @doc Updates the minimum if a number is removed.
sub(undefined, NewNum, _Tid) ->
    NewNum
;

sub(Min, Dels, Tid) ->
    % Delete min entry that has fallen out.
    Timestamp = ydb_ets_utils:max_timestamp(Dels)
  , ydb_ets_utils:delete_tuples(Tid, {inter_min, Timestamp})
    % Compute the new min.
  , InterMins = ydb_ets_utils:dump_tuples(Tid, inter_min)
  , NewMin = min(Min, get_mins(InterMins))
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
