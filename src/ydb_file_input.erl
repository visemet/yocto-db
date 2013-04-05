%% @author Max Hirschhorn <maxh@caltech.edu>

%% @doc Module containing functions used for reading tuples from files.
%%      Creates tuples from data within the files.
-module(ydb_file_input).
-behaviour(ydb_plan_node).

-export([start_link/2]).
-export([init/1, delegate/2, delegate/3]).

-include_lib("kernel/include/file.hrl").

-record(file_input, {
    io_device :: file:io_device()
  , batch_size :: integer()
  , poke_freq :: integer()
}).

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-spec start_link(Args :: list(), Options :: list()) ->
    {ok, Pid :: pid()}
  | ignore
  | {error, Error :: term()}
.

%% @doc TODO
start_link(Args, Options) ->
    ydb_plan_node:start_link(?MODULE, Args, Options)
.

%% ----------------------------------------------------------------- %%

-spec init(Args :: list()) ->
    {ok, State :: #file_input{}}
  | {error, {badarg, Term :: term}}
.

%% @doc TODO
init(Args) when is_list(Args) -> init(Args, #file_input{}).

-spec delegate(Request :: atom(), State :: #file_input{}) ->
    {ok, State :: #file_input{}}
.

%% @doc TODO
delegate(Request = {read}, State = #file_input{}) ->
    ydb_plan_node:relegate(
        erlang:self()
      , Request
      , [schema, timestamp]
    )

  , {ok, State}
;

delegate(_Request, State) ->
    {ok, State}
.

-spec delegate(
    Request :: atom()
  , State :: #file_input{}
  , Extras :: list()
) ->
    {ok, State :: #file_input{}}
.

%% @doc TODO
delegate(
    Request = {read}
  , State = #file_input{
        io_device = IoDevice
      , batch_size = BatchSize
      , poke_freq = PokeFreq
    }

  , _Extras = [Schema, Timestamp]
) ->
    case read(IoDevice, BatchSize) of
        {continue, Data} ->
            ydb_input_node_utils:push(
                ydb_input_node_utils:make_tuples(Timestamp, Schema, Data)
            )

          , io:format("data ~p~n", [Data])

          , timer:apply_after(
                PokeFreq
              , ydb_plan_node
              , relegate
              , [erlang:self(), Request]
            )

          , {ok, State}

      ; {done, Data} ->
            ydb_input_node_utils:push(
                ydb_input_node_utils:make_tuples(Timestamp, Schema, Data)
            )

          , file:close(IoDevice)

          , NewState = State#file_input{
                io_device=undefined
            }

          , {ok, NewState}
    end
;

delegate(_Request, State, _Extras) ->
    {ok, State}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init(list(), State :: #file_input{}) ->
    {ok, State :: #file_input{}}
  | {error, {badarg, Term :: term()}}
.

%% @doc TODO
init([], State = #file_input{}) ->
    post_init()

  , {ok, State}
;

init([{filename, Filename} | Args], State = #file_input{}) ->
    init(Args, State#file_input{io_device=open(Filename)})
;

init([{batch_size, BatchSize} | Args], State = #file_input{}) ->
    init(Args, State#file_input{batch_size=BatchSize})
;

init([{poke_freq, PollFreq} | Args], State = #file_input{}) ->
    init(Args, State#file_input{poke_freq=PollFreq})
;

init([Term | _Args], #file_input{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

-spec post_init() -> ok.

%% @doc TODO
post_init() -> ydb_plan_node:relegate(erlang:self(), {read}).

%% ----------------------------------------------------------------- %%

-spec open(Filename :: file:name_all() | iodata()) ->
    IoDevice :: file:io_device().

%% @doc Opens a particular file and returns the IO device.
open(Filename) ->
    {ok, IoDevice} = file:open(Filename, [read])
  , IoDevice
.

%% ----------------------------------------------------------------- %%

-spec read(IoDevice :: file:io_device(), BatchSize :: integer()) ->
    {done, Result :: list()}
  | {continue, Result :: list()}
.

%% @doc Reads from an IO device with a particular batch size.
read(IoDevice, BatchSize) ->
    read(IoDevice, BatchSize, [])
.

-spec read(
    IoDevice :: file:io_device()
  , BatchSize :: integer()
  , Result :: list()) ->
    {done, Result :: list()}
  | {continue, Result :: list()}
.

%% @doc Reads from an IO device with a particular batch size and list
%%      of results. TODO
read(_IoDevice, -1, Result) when is_list(Result) ->
    {done, lists:reverse(Result)}
;

read(_IoDevice, 0, Result) when is_list(Result) ->
    {continue, lists:reverse(Result)}
;

read(IoDevice, BatchSize, Result)
  when
    is_integer(BatchSize), BatchSize > 0
  , is_list(Result)
  ->
    case io:read(IoDevice, '') of
        {ok, Term} ->
            read(IoDevice, BatchSize - 1, [Term | Result])

      ; eof ->
            read(IoDevice, -1, Result)
    end
.

%% ----------------------------------------------------------------- %%
