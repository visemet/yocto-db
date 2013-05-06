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

%% @doc TODO
delegate(
    _Request = {diffs, Diffs}
  , State = #join{left_pid = LeftPid, right_pid = RightPid}
) ->
    NewState = lists:foldl(
        fun (Diff, S = #join{}) ->
            case get_owner(Diff, LeftPid, RightPid) of
                left -> received_left(Diff, S)

              ; right -> received_right(Diff, S)
            end
        end

      , State
      , Diffs
    )

  , {ok, NewState}
;

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

-spec received_left(LeftDiff :: ets:tid(), State :: join()) ->
    NewState :: join()
.

%% @doc TODO
received_left(LeftDiff, State = #join{
    left_history_size = LeftHistorySize
  , right_history_size = RightHistorySize
  , parity = Parity
  , left_diffs = LeftDiffs
  , right_diffs = RightDiffs
  , output_diffs = OutputDiffs
}) ->
    NumLeftDiffs = erlang:length(LeftDiffs)
  , NumRightDiffs = erlang:length(RightDiffs)

  , NewOutputDiffs = if
        NumLeftDiffs < NumRightDiffs + Parity -> OutputDiffs

        % Need to create new table
      ; NumLeftDiffs >= NumRightDiffs + Parity ->
            {ok, Tid} = ydb_ets_utils:create_diff_table(?MODULE)
          , lists:append(OutputDiffs, [Tid])
    end

  , DiffsTuple = {LeftDiff, RightDiffs, NewOutputDiffs}

    % Do forward join using right history size
  , left_backward_join(DiffsTuple, Parity, RightHistorySize)

    % Do backward join using left history size
  , left_forward_join(DiffsTuple, Parity, LeftHistorySize)

  , NewLeftDiffs = case evict_left_diffs(
        Parity
      , LeftHistorySize
      , NumRightDiffs
    ) of
        true -> erlang:tl(lists:append(LeftDiffs, [LeftDiff]))

      ; false -> lists:append(LeftDiffs, [LeftDiff])
    end

  , NewRightDiffs = if
        NumRightDiffs =:= 0 -> RightDiffs

      ; NumRightDiffs > 0 ->
            case evict_right_diffs(Parity, RightHistorySize, NumLeftDiffs) of
                true -> erlang:tl(RightDiffs)

              ; false -> RightDiffs
            end
    end

    % Define parity as `-1' when diff was evicted, and `0' otherwise
  , LeftParity = erlang:length(NewLeftDiffs) - NumLeftDiffs - 1
    % Define parity as `1' when diff was evicted, and `0' otherwise
  , RightParity = NumRightDiffs - erlang:length(NewRightDiffs)

  , NewParity = Parity + LeftParity + RightParity

  , if
        (Parity > 0 andalso LeftParity =:= -1)
        orelse (Parity < 0 andalso RightParity =:= 1) ->
            ydb_plan_node:notify(
                erlang:self()
              , {diffs, [erlang:hd(NewOutputDiffs)]}
            )

          , State#join{
                parity=NewParity
              , left_diffs=NewLeftDiffs
              , right_diffs=NewRightDiffs
              , output_diffs=erlang:tl(NewOutputDiffs)
            }

      ; true ->
            State#join{
                parity=NewParity
              , left_diffs=NewLeftDiffs
              , right_diffs=NewRightDiffs
              , output_diffs=NewOutputDiffs
            }
    end
.

-spec received_right(RightDiff :: ets:tid(), State :: join()) ->
    NewState :: join()
.

%% @doc TODO
received_right(RightDiff, State = #join{
    left_history_size = LeftHistorySize
  , right_history_size = RightHistorySize
  , parity = Parity
  , left_diffs = LeftDiffs
  , right_diffs = RightDiffs
  , output_diffs = OutputDiffs
}) ->
    NumLeftDiffs = erlang:length(LeftDiffs)
  , NumRightDiffs = erlang:length(RightDiffs)

  , NewOutputDiffs = if
        NumRightDiffs < NumLeftDiffs - Parity -> OutputDiffs

        % Need to create new table
      ; NumRightDiffs >= NumLeftDiffs - Parity ->
            {ok, Tid} = ydb_ets_utils:create_diff_table(?MODULE)
          , lists:append(OutputDiffs, [Tid])
    end

  , DiffsTuple = {LeftDiffs, RightDiff, NewOutputDiffs}

    % Do forward join using right history size
  , right_backward_join(DiffsTuple, Parity, LeftHistorySize)

    % Do backward join using left history size
  , right_forward_join(DiffsTuple, Parity, RightHistorySize)

  , NewRightDiffs = case evict_right_diffs(
        Parity
      , RightHistorySize
      , NumLeftDiffs
    ) of
        true -> erlang:tl(lists:append(RightDiffs, [RightDiff]))

      ; false -> lists:append(RightDiffs, [RightDiff])
    end

  , NewLeftDiffs = if
        NumLeftDiffs =:= 0 -> LeftDiffs

      ; NumLeftDiffs > 0 ->
            case evict_left_diffs(Parity, LeftHistorySize, NumRightDiffs) of
                true -> erlang:tl(LeftDiffs)

              ; false -> LeftDiffs
            end
    end

    % Define parity as `-1' when diff was evicted, and `0' otherwise
  , LeftParity = erlang:length(NewLeftDiffs) - NumLeftDiffs
    % Define parity as `1' when diff was evicted, and `0' otherwise
  , RightParity = NumRightDiffs - erlang:length(NewRightDiffs) + 1

  , NewParity = Parity + LeftParity + RightParity

  , if
        (Parity > 0 andalso LeftParity =:= -1)
        orelse (Parity < 0 andalso RightParity =:= 1) ->
            ydb_plan_node:notify(
                erlang:self()
              , {diffs, [erlang:hd(NewOutputDiffs)]}
            )

          , State#join{
                parity=NewParity
              , left_diffs=NewLeftDiffs
              , right_diffs=NewRightDiffs
              , output_diffs=erlang:tl(NewOutputDiffs)
            }

      ; true ->
            State#join{
                parity=NewParity
              , left_diffs=NewLeftDiffs
              , right_diffs=NewRightDiffs
              , output_diffs=NewOutputDiffs
            }
    end
.

%% ----------------------------------------------------------------- %%

-spec evict_left_diffs(
    Offset :: integer()
  , HistorySize :: pos_integer()
  , NumRightDiffs :: non_neg_integer()
) ->
    boolean()
.

%% @doc TODO
evict_left_diffs(Offset, HistorySize, NumRightDiffs) ->
    NumRightDiffs + Offset >= 1 + HistorySize
.

-spec evict_right_diffs(
    Offset :: integer()
  , HistorySize :: pos_integer()
  , NumLeftDiffs :: non_neg_integer()
) ->
    boolean()
.

%% @doc TODO
evict_right_diffs(Offset, HistorySize, NumLeftDiffs) ->
    NumLeftDiffs - Offset >= 1 + HistorySize
.


%% ----------------------------------------------------------------- %%

%% @doc TODO
left_backward_join(
    {LeftDiff, RightDiffs, OutputDiffs}
  , Offset
  , HistorySize % Right
) when
    is_integer(Offset)
  , is_integer(HistorySize) > 0
  ->
    {LeftIndex, _RightIndex} = get_index(Offset)

  , Start = LeftIndex - HistorySize
  , if
        Start < 1 ->
            join_diffs(
                LeftDiff
              , lists:sublist(RightDiffs, 1, LeftIndex)
              , lists:nth(LeftIndex, OutputDiffs)
            )

      ; Start >= 1 ->
            join_diffs(
                LeftDiff
              , lists:sublist(RightDiffs, Start, HistorySize + 1)
              , lists:nth(LeftIndex, OutputDiffs)
            )
    end
.

%% @doc TODO
right_backward_join(
    {LeftDiffs, RightDiff, OutputDiffs}
  , Offset
  , HistorySize % Left
) when
    is_integer(Offset)
  , is_integer(HistorySize) > 0
  ->
    {_LeftIndex, RightIndex} = get_index(Offset)

  , Start = RightIndex - HistorySize
  , if
        Start < 1 ->
            join_diffs(
                lists:sublist(LeftDiffs, 1, RightIndex)
              , RightDiff
              , lists:nth(RightIndex, OutputDiffs)
            )

      ; Start >= 1 ->
            join_diffs(
                lists:sublist(LeftDiffs, Start, HistorySize + 1)
              , RightDiff
              , lists:nth(RightIndex, OutputDiffs)
            )
    end
.

%% ----------------------------------------------------------------- %%

%% @doc TODO
left_forward_join(
    {LeftDiff, RightDiffs, OutputDiffs}
  , Offset
  , HistorySize % Left
) when
    is_integer(Offset)
  , is_integer(HistorySize) > 0
  ->
    {LeftIndex, _RightIndex} = get_index(Offset)

  , Start = LeftIndex + 1
  , if
        Start < erlang:length(RightDiffs) -> pass

      ; Start >= erlang:length(RightDiffs) ->
            lists:foreach(
                fun ({RightDiff, OutputDiff}) ->
                    cross_product(LeftDiff, RightDiff, OutputDiff)
                end

              , lists:sublist(
                    lists:zip(RightDiffs, OutputDiffs)
                  , Start
                  , HistorySize
                )
            )
    end
.

%% @doc TODO
right_forward_join(
    {LeftDiffs, RightDiff, OutputDiffs}
  , Offset
  , HistorySize % Right
) when
    is_integer(Offset)
  , is_integer(HistorySize) > 0
  ->
    {_LeftIndex, RightIndex} = get_index(Offset)

  , Start = RightIndex + 1
  , if
        Start < erlang:length(LeftDiffs) -> pass

      ; Start >= erlang:length(LeftDiffs) ->
            lists:foreach(
                fun ({LeftDiff, OutputDiff}) ->
                    cross_product(LeftDiff, RightDiff, OutputDiff)
                end

              , lists:sublist(
                    lists:zip(LeftDiffs, OutputDiffs)
                  , Start
                  , HistorySize
                )
            )
    end
.

%% ----------------------------------------------------------------- %%

-spec get_index(Offset :: integer()) ->
    {LeftIndex :: pos_integer(), RightIndex :: pos_integer()}
.

%% @doc TODO
get_index(Offset) when is_integer(Offset), Offset < 0 ->
    {-Offset + 1, 1}
;

get_index(Offset) when is_integer(Offset), Offset >= 0 ->
    {1, Offset + 1}
.

%% ----------------------------------------------------------------- %%

-spec join_diffs(
    LeftDiff :: ets:tid()
  , RightDiffs :: [ets:tid()]
  , OutputDiff :: ets:tid()
) ->
    ok
; (
    LeftDiffs :: [ets:tid()]
  , RightDiff :: ets:tid()
  , OutputDiff :: ets:tid()
) ->
    ok
.

%% @doc TODO
join_diffs(LeftDiff, RightDiffs, OutputDiff) when is_list(RightDiffs) ->
    lists:foreach(
        fun (RightDiff) ->
            cross_product(LeftDiff, RightDiff, OutputDiff)
        end

      , RightDiffs
    )
;

join_diffs(LeftDiffs, RightDiff, OutputDiff) when is_list(LeftDiffs) ->
    lists:foreach(
        fun (LeftDiff) ->
            cross_product(LeftDiff, RightDiff, OutputDiff)
        end

      , LeftDiffs
    )
.

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

-spec get_owner(ets:tid(), pid(), pid()) -> 'left' | 'right'.

%% @doc Returns the owner of the specified, i.e. `left' or `right'.
get_owner(Diff, LeftPid, RightPid) ->
    case ets:info(Diff, owner) of
        Owner when is_pid(Owner), Owner =:= LeftPid -> left

      ; Owner when is_pid(Owner), Owner =:= RightPid -> right
    end
.

%% ----------------------------------------------------------------- %%
