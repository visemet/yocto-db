%% @author Angela Gong <anjoola@anjoola.com>

%% @doc Module containing functions used for writing tuples to files.
-module(ydb_file_output).
-behaviour(ydb_plan_node).

-export([start_link/2]).
-export([init/1, delegate/2, delegate/3, compute_schema/2]).

-include_lib("kernel/include/file.hrl").

%% @headerfile "ydb_plan_node.hrl"
-include_lib("ydb_plan_node.hrl").

% Testing for private functions.
-ifdef(TEST).
-export([open/1, close/1, write/2]).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(file_output, {
    filename="out.dta" :: string()
}).

-type file_output() :: #file_output{
    filename :: string()}.
%% Internal file output node state.

-type option() ::
    {filename, Filename :: string()}.
%% Options for the file output:
%% <ul>
%%   <li><code>{filename, Filename}</code> - Output to a file named
%%       <code>Filename</code>.</li>
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

%% ----------------------------------------------------------------- %%

-spec init(Args :: [option()]) ->
    {ok, State :: file_output()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Initializes the output node's internal state.
init(Args) when is_list(Args) -> init(Args, #file_output{}).

-spec delegate(Request :: atom(), State :: file_output()) ->
    {ok, State :: file_output()}
.

%% @private
%% @doc Accepts tuples and writes them to the file.
delegate(
    _Request = {tuple, Tuple}
  , State = #file_output{filename=Filename}
) ->
    write(Filename, Tuple)
  , {ok, State}
;

% Accepts a list of tuples.
delegate(
    _Request = {tuples, Tuples}
  , State = #file_output{filename=Filename}
) when
    is_list(Tuples)
->
    write(Filename, Tuples)
  , {ok, State}
;

delegate(_Request = {info, Message}, State) ->
    delegate(Message, State)
;

delegate(_Request, State) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec delegate(
    Request :: atom()
  , State :: file_output()
  , Extras :: list()
) ->
    {ok, NewState :: file_output()}
.

%% @private
%% @doc Not implmented.
delegate(_Request, State, _Extras) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec compute_schema(
    InputSchemas :: [ydb_plan_node:ydb_schema()]
  , State :: file_output()
) ->
    {ok, OutputSchema :: ydb_plan_node:ydb_schema()}
  | {error, {badarg, InputSchemas :: [ydb_plan_node:ydb_schema()]}}
.

%% @doc Returns the output schema of the file output node based upon
%%      the supplied input schemas. Expects a single schema.
compute_schema([Schema], #file_output{}) ->
    {ok, Schema}
;

compute_schema(Schemas, #file_output{}) ->
    {error, {badarg, Schemas}}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: file_output()) ->
    {ok, State :: file_output()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Parses initializing arguments to set up the internal state of
%%      the output node.
init([], State = #file_output{}) ->
    {ok, State}
;

init([{filename, Filename} | Args], State = #file_output{}) ->
    init(Args, State#file_output{filename=Filename})
;

init([Term | _Args], #file_output{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

-spec open(Filename :: string()) ->
    IoDevice :: file:io_device().

%% @private
%% @doc Opens a particular file and returns the IO device.
open(Filename) ->
    {ok, IoDevice} = file:open(Filename, [append])
  , IoDevice
.

%% @private
%% @doc Closes an IO device.
-spec close(IoDevice :: file:io_device()) ->
    ok | {error, Reason :: term()}
.

close(IoDevice) ->
    file:close(IoDevice)
.

%% ----------------------------------------------------------------- %%

-spec write(
    Filename :: string()
  , Data :: ydb_plan_node:ydb_tuple() | [ydb_plan_node:ydb_tuple()]
) ->
    'ok'
.

%% @private
%% @doc Writes tuples to a file. Flushes the output by opening and
%%      closing the file.
write(Filename, Data) when is_tuple(Data) ->
    IoDevice = open(Filename)
  , io:fwrite(IoDevice, "~w.~n", [Data])
  , close(IoDevice)
;

write(Filename, Data) when is_list(Data) ->
    IoDevice = open(Filename)
  , lists:foreach(fun(Tuple) ->
        io:fwrite(IoDevice, "~w.~n", [Tuple]) end, Data
    )
  , close(IoDevice)      
.


%%% =============================================================== %%%
%%%  private tests                                                  %%%
%%% =============================================================== %%%

-ifdef(TEST).
init_test() ->
    ?assertMatch(
        {ok, #file_output{filename="test_name"}}
      , init([], #file_output{filename="test_name"})
    )
  , ?assertMatch(
        {ok, #file_output{}}
      , init([], #file_output{})
    )
  , ?assertMatch(
        {error, {badarg, bad}}
      , init([bad], #file_output{})
    )
.
-endif.

%% ----------------------------------------------------------------- %%
