%% @author Max Hirschhorn <maxh@caltech.edu>

%% @doc Module for creating a sliding window of a stream, by amount of
%%      time. Creates a diff of what tuples were added and removed.
-module(ydb_time_window).
-behaviour(ydb_plan_node).

-export([start_link/2, start_link/3]).
-export([init/1, delegate/2, delegate/3, compute_schema/2]).

-record(time_window, {
    size=5000000 :: pos_integer() % in milliseconds
  , pulse=1000000 :: pos_integer() % in milliseconds

  , first :: 'undefined' | ets:tid()
  , last :: 'undefined' | ets:tid()

  , diffs=[] :: [ets:tid()]
}).

-type time_window() :: #time_window{
    size :: pos_integer()
  , pulse :: pos_integer()

  , first :: 'undefined' | ets:tid()
  , last :: 'undefined' | ets:tid()

  , diffs :: [ets:tid()]
}.
%% Internal state of time window node.

-type time_unit() ::
    'micro_sec' | 'milli_sec' | 'sec' | 'min' | 'hour'
.
%% TODO

-type option() ::
    {size, {Size :: pos_integer(), time_unit()}}
  | {pulse, {Pulse :: pos_integer(), time_unit()}}
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
    {ok, State :: time_window()}
  | {error, {badarg, Term :: term()}}
.

%% @doc Initializes the internal state of the window node.
init(Args) when is_list(Args) -> init(Args, #time_window{}).

-spec delegate(Request :: term(), State :: time_window()) ->
    {ok, State :: time_window()}
.

%% @doc TODO
delegate(
    _Request = {tuples, Tuples}
  , State = #time_window{}
) ->
    {ok, NewState}
;

delegate(_Request = {info, Message}, State = #time_window{}) ->
    delegate(Message, State)
;

delegate(_Request, State) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec delegate(
    Request :: term()
  , State :: time_window()
  , Extras :: list()
) ->
    {ok, NewState :: time_window()}
.

%% @doc Does nothing.
delegate(_Request, State, _Extras) ->
    {ok, State}
.

-spec compute_schema(
    InputSchemas :: [ydb_plan_node:ydb_schema()]
  , State :: time_window()
) ->
    {ok, OutputSchema :: ydb_plan_node:ydb_schema()}
  | {error, {badarg, InputSchemas :: [ydb_plan_node:ydb_schema()]}}
.

%% @doc Returns the output schema of the row window based upon the
%%      supplied input schemas. Expects a single schema.
compute_schema([Schema], #time_window{}) ->
    {ok, Schema}
;

compute_schema(Schemas, #time_window{}) ->
    {error, {badarg, Schemas}}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: time_window()) ->
    {ok, NewState :: time_window()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Initializes internal state of the window node.
init([], State = #time_window{}) ->
    {ok, State}
;

init([Term | _Args], #time_window{}) ->
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
