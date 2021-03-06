%% @author Max Hirschhorn <maxh@caltech.edu>

%% @doc Module for creating a sliding window of a stream, by number of
%%      rows. Creates a diff of what tuples were added and removed.
-module(ydb_row_window).
-behaviour(ydb_plan_node).

-export([start_link/2, start_link/3]).
-export([init/1, delegate/2, delegate/3, compute_schema/2]).

-record(row_window, {
    size=undefined :: 'undefined' | pos_integer()
  , pulse=undefined :: 'undefined' | pos_integer()

  , remain=1 :: non_neg_integer()

  , first :: 'undefined' | ets:tid()
  , last :: 'undefined' | ets:tid()

  , diffs=[] :: [ets:tid()]
}).

-type row_window() :: #row_window{
    size :: 'undefined' | pos_integer()
  , pulse :: 'undefined' | pos_integer()

  , remain :: non_neg_integer()

  , first :: 'undefined' | ets:tid()
  , last :: 'undefined' | ets:tid()

  , diffs :: [ets:tid()]
}.
%% Internal state of row window node.

-type option() ::
    {size, {Size :: pos_integer(), rows}}
  | {pulse, {Pulse :: pos_integer(), rows}}
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

%% @doc Starts the window node in the supervisor hierarchy.
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

%% @doc Starts the window node in the supervisor hierarchy with a
%%      registered name.
start_link(Name, Args, Options) ->
    ydb_plan_node:start_link(Name, ?MODULE, Args, Options)
.

%% ----------------------------------------------------------------- %%

-spec init(Args :: [option()]) ->
    {ok, State :: row_window()}
  | {error, {badarg, Term :: term()}}
.

%% @doc Initializes the internal state of the window node.
init(Args) when is_list(Args) -> init(Args, #row_window{}).

-spec delegate(Request :: term(), State :: row_window()) ->
    {ok, State :: row_window()}
.

%% @doc TODO
delegate(
    _Request = {tuples, Tuples}
  , State = #row_window{}
) ->
    NewState = lists:foldl(
        fun (Tuple, S = #row_window{
            remain = 0
          , first = First
          , diffs = Diffs
        }) ->
            ydb_plan_node:notify(erlang:self(), {diffs, [First]})

          , {ok, NewLast} = ydb_ets_utils:create_diff_table(?MODULE)
          , NewDiffs = shift_left(Diffs, NewLast)
          , NewFirst = get_first(NewDiffs)

            % Insert PLUS (`+') tuple into `NewFirst'
          , ydb_ets_utils:add_diffs(NewFirst, '+', row_window, Tuple)

            % Insert MINUS (`-') tuple into `NewLast'
          , ydb_ets_utils:add_diffs(NewLast, '-', row_window, Tuple)

          , NewRemain = S#row_window.pulse - 1

          , S#row_window{
                remain=NewRemain
              , first=NewFirst
              , last=NewLast
              , diffs=NewDiffs
            }

          ; (Tuple, S = #row_window{
            remain = Remain
          , first = First
          , last = Last
        }) when Remain > 0 ->
            % Insert PLUS (`+') tuple into `First'
            ydb_ets_utils:add_diffs(First, '+', row_window, Tuple)

            % Insert MINUS (`-') tuple into `Last'
          , ydb_ets_utils:add_diffs(Last, '-', row_window, Tuple)

          , NewRemain = Remain - 1

          , S#row_window{
                remain=NewRemain
            }
        end

      , State
      , Tuples
    )

  , {ok, NewState}
;

delegate(_Request = {info, Message}, State = #row_window{}) ->
    delegate(Message, State)
;

delegate(_Request, State) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec delegate(
    Request :: term()
  , State :: row_window()
  , Extras :: list()
) ->
    {ok, NewState :: row_window()}
.

%% @doc Does nothing.
delegate(_Request, State, _Extras) ->
    {ok, State}
.

-spec compute_schema(
    InputSchemas :: [ydb_plan_node:ydb_schema()]
  , State :: row_window()
) ->
    {ok, OutputSchema :: ydb_plan_node:ydb_schema()}
  | {error, {badarg, InputSchemas :: [ydb_plan_node:ydb_schema()]}}
.

%% @doc Returns the output schema of the row window based upon the
%%      supplied input schemas. Expects a single schema.
compute_schema([Schema], #row_window{}) ->
    {ok, Schema}
;

compute_schema(Schemas, #row_window{}) ->
    {error, {badarg, Schemas}}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: row_window()) ->
    {ok, NewState :: row_window()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Initializes internal state of the window node.
init([], State = #row_window{}) ->
    {ok, State}
;

init(
    [{size, {Size, rows}} | Args]
  , State = #row_window{pulse = undefined}
) ->
    init(Args, State#row_window{size=Size})
;

init(
    [{size, {Size, rows}} | Args]
  , State = #row_window{pulse = Pulse}
) ->
    Diffs = lists:map(
        fun (_SeqNum) ->
            {ok, Tid} = ydb_ets_utils:create_diff_table(?MODULE)
          , Tid
        end

      , lists:seq(0, Size div Pulse) % `floor(Size / Pulse) + 1' diffs
    )

  , init(Args, State#row_window{
        size=Size
      , first=get_first(Diffs)
      , last=get_last(Diffs)
      , diffs=Diffs
    })
;

init(
    [{pulse, {Pulse, rows}} | Args]
  , State = #row_window{size = undefined}
) ->
    init(Args, State#row_window{pulse=Pulse})
;

init(
    [{pulse, {Pulse, rows}} | Args]
  , State = #row_window{size = Size}
) ->
    Diffs = lists:map(
        fun (_SeqNum) ->
            {ok, Tid} = ydb_ets_utils:create_diff_table(?MODULE)
          , Tid
        end

      , lists:seq(0, Size div Pulse) % `floor(Size / Pulse) + 1' diffs
    )

  , init(Args, State#row_window{
        pulse=Pulse
      , first=get_first(Diffs)
      , last=get_last(Diffs)
      , diffs=Diffs
    })
;

init([Term | _Args], #row_window{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

-spec get_first([ets:tid()]) -> ets:tid().

%% @doc Returns the first diff.
get_first(Diffs) ->
    erlang:hd(Diffs)
.

-spec set_first(ets:tid(), [ets:tid()], pos_integer()) -> [ets:tid()].

%% @doc Replaces the first diff with specified diff, or prepends if the
%%      length of the list is less than the maximum size.
set_first(NewFirst, Diffs, MaxSize) ->
    Size = erlang:length(Diffs)

  , if
        Size < MaxSize -> [NewFirst|Diffs]

      ; Size =:= MaxSize -> [NewFirst|erlang:tl(Diffs)]
    end
.

-spec get_last([ets:tid()]) -> ets:tid().

%% @doc Returns the last diff.
get_last(Diffs) ->
    lists:last(Diffs)
.

-spec set_last(ets:tid(), [ets:tid()], pos_integer()) -> [ets:tid()].

%% @doc Replaces the last diff with specified diff, or appends if the
%%      length of the list is less than the maximum size.
set_last(NewLast, Diffs, MaxSize) ->
    Size = erlang:length(Diffs)

  , if
        Size < MaxSize -> lists:append(Diffs, [NewLast])

      ; Size =:= MaxSize ->
        lists:append(lists:sublist(Diffs, Size - 1), [NewLast])
    end
.

%% ----------------------------------------------------------------- %%

-spec shift_left([ets:tid()], ets:tid()) -> [ets:tid()].

%% @doc Removes the first diff and appends the specific one.
shift_left(Diffs, NewLast) ->
    lists:append(erlang:tl(Diffs), [NewLast])
.

%% ----------------------------------------------------------------- %%
