%% @author Max Hirschhorn <maxh@caltech.edu>

%% @doc Module for taking the Cartesian product of two relations.
%%      Creates a diff of what tuples were since added and removed.
-module(ydb_join).
-behaviour(ydb_plan_node).

-export([start_link/2, start_link/3]).
-export([init/1, delegate/2, delegate/3, compute_schema/2]).

%% @headerfile "ydb_time.hrl"
-include("ydb_time.hrl").

%% @headerfile "ydb_plan_node.hrl"
-include("ydb_plan_node.hrl").

-record(join, {
    left_history_size=1 :: pos_integer()
  , right_history_size=1 :: pos_integer()

  , left_pid :: 'undefined' | pid()
  , right_pid :: 'undefined' | pid()

  , left_diffs=[] :: [ets:tid()]
  , right_diffs=[] :: [ets:tid()]

  , parity=0 :: integer()

  , output_diffs=[] :: [ets:tid()]
}).

-type join() :: #join{
    left_history_size :: pos_integer()
  , right_history_size :: pos_integer()

  , left_pid :: 'undefined' | pid()
  , right_pid :: 'undefined' | pid()

  , left_diffs :: [ets:tid()]
  , right_diffs :: [ets:tid()]

  , parity :: integer()

  , output_diffs :: [ets:tid()]
}.
%% Internal state of join node.

-type window_unit() :: time_unit() | 'row'.

-type option() ::
    {left,
        {size, {window_unit(), Size :: pos_integer()}}
      , {pulse, {window_unit(), Pulse :: pos_integer()}}
    }

  | {right,
        {size, {window_unit(), Size :: pos_integer()}}
      , {pulse, {window_unit(), Pulse :: pos_integer()}}
    }
.
%% TODO

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-spec start_link(
    Args :: [option()]
  , Options :: list()
) ->
    {ok, Pid :: pid()}
  | ignore
  | {error, Error :: term()}
.

%% @doc Starts the join node in the supervisor hierarchy.
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

%% @doc Starts the join node in the supervisor hierarchy with a
%%      registered name.
start_link(Name, Args, Options) ->
    ydb_plan_node:start_link(Name, ?MODULE, Args, Options)
.

%% ----------------------------------------------------------------- %%

-spec init(Args :: [option()]) ->
    {ok, State :: join()}
  | {error, {badarg, Term :: term()}}
.

%% @doc Initializes the internal state of the join node.
init(Args) when is_list(Args) -> init(Args, #join{}).

-spec delegate(Request :: term(), State :: join()) ->
    {ok, NewState :: join()}
.

delegate(
    _Request = {get_listenees, Listenees}
  , State = #join{}
) ->
    [LeftPid, RightPid] = lists:map(
        fun (PidFun) when is_function(PidFun, 0) ->
            PidFun()
        end

      , Listenees
    )

  , {ok, State#join{left_pid=LeftPid, right_pid=RightPid}}
;

delegate(_Request = {info, Message}, State = #join{}) ->
    delegate(Message, State)
;

delegate(_Request, State) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec delegate(
    Request :: term()
  , State :: join()
  , Extras :: list()
) ->
    {ok, NewState :: join()}
.

%% @doc Does nothing.
delegate(_Request, State, _Extras) ->
    {ok, State}
.

-spec compute_schema(
    InputSchemas :: [ydb_plan_node:ydb_schema()]
  , State :: join()
) ->
    {ok, OutputSchema :: ydb_plan_node:ydb_schema()}
  | {error, {badarg, InputSchemas :: [ydb_plan_node:ydb_schema()]}}
.

%% @doc Returns the output schema of the join based upon the supplied
%%      input schemas. Expects two schemas, denoted left and right.
compute_schema([LeftSchema, RightSchema], #join{}) ->
    OutputSchema = lists:append(LeftSchema, RightSchema)

  , {ok, OutputSchema}
;

compute_schema(Schemas, #join{}) ->
    {error, {badarg, Schemas}}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: join()) ->
    {ok, NewState :: join()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Initializes internal state of the join node.
init([], State = #join{}) ->
    {ok, State}
;

init(
    [{left, {size, {row, Size}}, {pulse, {row, Pulse}}} | Args]
  , State = #join{})
  ->
    LeftHistorySize = Size div Pulse

  , init(Args, State#join{left_history_size=LeftHistorySize})
;

init(
    [{left, {size, SizeTuple}, {pulse, PulseTuple}} | Args]
  , State = #join{})
  ->
    Size = ydb_time_utils:convert_time(SizeTuple)
  , Pulse = ydb_time_utils:convert_time(PulseTuple)

  , LeftHistorySize = Size div Pulse

  , init(Args, State#join{left_history_size=LeftHistorySize})
;

init(
    [{right, {size, {row, Size}}, {pulse, {row, Pulse}}} | Args]
  , State = #join{})
  ->
    RightHistorySize = Size div Pulse

  , init(Args, State#join{right_history_size=RightHistorySize})
;

init(
    [{right, {size, SizeTuple}, {pulse, PulseTuple}} | Args]
  , State = #join{})
  ->
    Size = ydb_time_utils:convert_time(SizeTuple)
  , Pulse = ydb_time_utils:convert_time(PulseTuple)

  , RightHistorySize = Size div Pulse

  , init(Args, State#join{right_history_size=RightHistorySize})
;

init([Term | _Args], #join{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

-spec cross_product(
    LeftDiff :: ets:tid()
  , RightDiff :: ets:tid()
  , OutputDiff :: ets:tid()
) ->
    ok
.

%% @doc Takes the signed Cartesian product of two diffs.
cross_product(LeftDiff, RightDiff, OutputDiff) ->
    {LeftPlus, LeftMinus} = ydb_ets_utils:extract_diffs([LeftDiff])
  , {RightPlus, RightMinus} = ydb_ets_utils:extract_diffs([RightDiff])

    % PLUS x PLUS -> PLUS
  , lists:foreach(
        fun (LeftTuple = #ydb_tuple{}) ->
            OutputTuples = lists:map(
                fun (RightTuple = #ydb_tuple{}) ->
                    merge_tuples(LeftTuple, RightTuple)
                end

              , RightPlus
            )

          , ydb_ets_utils:add_diffs(OutputDiff, '+', join, OutputTuples)
        end

      , LeftPlus
    )

    % MINUS x MINUS -> MINUS
  , lists:foreach(
        fun (LeftTuple = #ydb_tuple{}) ->
            OutputTuples = lists:map(
                fun (RightTuple = #ydb_tuple{}) ->
                    merge_tuples(LeftTuple, RightTuple)
                end

              , RightMinus
            )

          , ydb_ets_utils:add_diffs(OutputDiff, '-', join, OutputTuples)
        end

      , LeftMinus
    )
.

-spec merge_tuples(
    LeftTuple :: ydb_plan_node:ydb_tuple()
  , RightTuple :: ydb_plan_node:ydb_tuple()
) ->
    OutputTuple :: ydb_plan_node:ydb_tuple()
.

%% @doc Appends two tuples together.
merge_tuples(
    #ydb_tuple{data = LeftData}
  , #ydb_tuple{data = RightData}
) ->
    Timestamp = ydb_time_utils:get_curr_time()
  , Data = erlang:list_to_tuple(
        lists:append(
            erlang:tuple_to_list(LeftData)
          , erlang:tuple_to_list(RightData)
        )
    )

  , #ydb_tuple{timestamp=Timestamp, data=Data}
.

%% ----------------------------------------------------------------- %%
