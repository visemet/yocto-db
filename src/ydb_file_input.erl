%% @author Max Hirschhorn <maxh@caltech.edu>

%% @doc Module containing functions used for reading tuples from files.
%%      Creates tuples from data within the files.
-module(ydb_file_input).
-behaviour(ydb_plan_node).

-export([start_link/2, start_link/3, do_read/1]).
-export([init/1, delegate/2, delegate/3, compute_schema/2]).

% Testing for private functions.
-ifdef(TEST).
-export([open/1,read/2,read/3]).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("kernel/include/file.hrl").

-record(file_input, {
    io_device :: file:io_device()
  , batch_size=5 :: integer()
  , poke_freq=1 :: integer()
}).

-type file_input() :: #file_input{
    io_device :: undefined | file:io_device()
  , batch_size :: integer()
  , poke_freq :: integer()}.
% Internal file input node state.
    
-type option() ::
    {filename, Filename :: string()}
  | {batch_size, BatchSize :: integer()}
  | {poke_freq, PokeFreq :: integer()}.
%% Options for the file input:
%% <ul>
%%   <li><code>{filename, Filename}</code> - Read input from the file
%%       <code>Filename</code>.</li>
%%   <li><code>{batch_size, BatchSize}</code> - Reads
%%       <code>BatchSize</code> tuples at a time from the file.</li>
%%   <li><code>{poke_freq, PokeFreq}</code> - Read the file every
%%       <code>PokeFreq</code> milliseconds.</li>
%% </ul>

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-spec start_link(Args :: [option()], Options :: list()) ->
    {ok, Pid :: pid()}
  | ignore
  | {error, Error :: term()}
.

%% @doc Starts the input node in the supervisor hierarchy.
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

%% @doc Starts the input node in the supervisor hierarchy with a
%%      registered name.
start_link(Name, Args, Options) ->
    ydb_plan_node:start_link(Name, ?MODULE, Args, Options)
.

-spec do_read(Pid :: pid()) -> ok.

%% @doc Tells a particular file input process to start reading from
%%      the file.
do_read(Pid) when is_pid(Pid) ->
    ydb_plan_node:relegate(Pid, {read})
.

%% ----------------------------------------------------------------- %%

-spec init(Args :: [option()]) ->
    {ok, State :: #file_input{}}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Initializes the input node's internal state.
init(Args) when is_list(Args) ->
    init(Args, #file_input{})
;

init(_Args) ->
    {error, {badarg, not_options_list}}
.

%% ----------------------------------------------------------------- %%

-spec delegate(Request :: atom(), State :: file_input()) ->
    {ok, State :: file_input()}
.

%% @private
%% @doc Accepts requests for the next tuple in the file and
%%      sends out a request for the relevant schema and timestamp,
%%      in order to construct the tuple.
delegate(Request = {read}, State = #file_input{}) ->
    ydb_plan_node:relegate(
        erlang:self()
      , Request
      , [schema, timestamp]
    )

  , {ok, State}
;

%% ----------------------------------------------------------------- %%

delegate(_Request, State) ->
    {ok, State}
.

-spec delegate(
    Request :: atom()
  , State :: file_input()
  , Extras :: list()
) ->
    {ok, State :: file_input()}
.

%% @private
%% @doc Reads the next line in a file, converts it to a tuple, and
%%      pushes it to the next node in the stream. Closes the file
%%      when the end is reached. Frequency of file reads is
%%      determined by <tt>poke_freq</tt>, in ms.
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
            Tuples = ydb_input_node_utils:make_tuples(Timestamp, Schema, Data)
          , ydb_plan_node:send_tuples(erlang:self(), Tuples)
          , timer:apply_after(
                PokeFreq
              , ydb_plan_node
              , relegate
              , [erlang:self(), Request]
            )

          , {ok, State}

      ; {done, Data} ->
            Tuples = ydb_input_node_utils:make_tuples(Timestamp, Schema, Data)
          , ydb_plan_node:send_tuples(erlang:self(), Tuples)
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

%% ----------------------------------------------------------------- %%

-spec compute_schema(
    InputSchemas :: [ydb_plan_node:ydb_schema()]
  , State :: file_input()
) ->
    {error, {badarg, InputSchemas :: [ydb_plan_node:ydb_schema()]}}
.

%% @doc Returns the output schema of the file input node based upon the
%%      supplied input schemas. Not supported, as the schema should
%%      only be defined during initialization.
compute_schema(Schemas, #file_input{}) ->
    {error, {badarg, Schemas}}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: file_input()) ->
    {ok, State :: file_input()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Parses initializing arguments to set up the internal state of
%%      the input node.
init([], State = #file_input{}) ->
    {ok, State}
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

-spec open(Filename :: string()) ->
    IoDevice :: file:io_device().

%% @private
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

%% @private
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

%% @private
%% @doc Reads from an IO device of a particular batch size appends to a 
%%      list of results.
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

%%% =============================================================== %%%
%%%  private tests                                                  %%%
%%% =============================================================== %%%

-ifdef(TEST).
init_test() ->
    ?assertMatch(
        {ok, #file_input{batch_size=34}}
      , init([], #file_input{batch_size=34})
    )
  , ?assertMatch(
        {ok, #file_input{batch_size=3}}
      , init(
            [{filename, "../data/read_test_helper.dta"}]
          , #file_input{batch_size=3}
        )
    )
  , ?assertMatch(
        {ok, #file_input{poke_freq=343}}
      , init(
            [{filename, "../data/read_test_helper.dta"}]
          , #file_input{poke_freq=343}
        )
    )
  , ?assertMatch(
        {error, {badarg, bad}}
      , init([bad], #file_input{})
    )
.
-endif.

%% ----------------------------------------------------------------- %%
