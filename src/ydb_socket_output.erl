%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc This module receives tuples from the query planner and outputs
%%      it to a socket.
-module(ydb_socket_output).
-behaviour(ydb_plan_node).

-export([start_link/2]).
-export([init/1, delegate/2, delegate/3, compute_schema/2]).

% Testing for private functions.
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(socket_output, {
    port_no=9001 :: integer()
  , socket :: port()
  , address :: term()
}).

-type socket_output() :: #socket_output{
    port_no :: integer()
  , socket :: undefined | port()
  , address :: undefined | term()}.
%% Internal socket output node state.

-type option() ::
    {port_no, integer()}
  | {address, term()}.
%% Options for the socket output:
%% <ul>
%%   <li><code>{port_no, PortNo}</code> - Outputs to the port specified
%%       by <code>PortNo</code>.</li>
%%   <li><code>{address, Address}</code> - Outputs to the address
%%       specified by <code>Address</code>.</li>
%% </ul>

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-spec start_link(Args :: [option()], Options :: list()) ->
    {ok, Pid :: pid()}
  | ignore
  | {error, Error :: term()}
.

%% @doc Starts the output node in the supervisor hierarchy.
start_link(Args, Options) ->
    ydb_plan_node:start_link(?MODULE, Args, Options)
.

%% ----------------------------------------------------------------- %%

-spec init(Args :: [option()]) ->
    {ok, State :: socket_output()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Initializes the output node's internal state.
init(Args) when is_list(Args) -> init(Args, #socket_output{}).

%% ----------------------------------------------------------------- %%

-spec delegate(Request :: atom(), State :: socket_output()) ->
    {ok, State :: #socket_output{}}
.

%% @private
%% @doc Sends received tuples out through a socket at the port
%%      and address initially specified.
delegate(
    _Request = {tuple, Tuple}
  , State = #socket_output{socket = Sock}
) ->
    ydb_socket_utils:send_tuples(Sock, [Tuple])
  , {ok, State}
;

delegate(
    _Request = {tuples, Tuples}
  , State = #socket_output{socket = Sock}
) when is_list(Tuples)
->
    ydb_socket_utils:send_tuples(Sock, Tuples)
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
  , State :: socket_output()
  , Extras :: list()
) ->
    {ok, NewState :: socket_output()}
.

%% @private
%% @doc Not implemented.
delegate(_Request, State, _Extras) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec compute_schema(
    InputSchemas :: [ydb_plan_node:ydb_schema()]
  , State :: socket_output()
) ->
    {ok, OutputSchema :: ydb_plan_node:ydb_schema()}
  | {error, {badarg, InputSchemas :: [ydb_plan_node:ydb_schema()]}}
.

%% @doc Returns the output schema of the socket output node based upon
%%      the supplied input schemas. Expects a single schema.
compute_schema([Schema], #socket_output{}) ->
    {ok, Schema}
;

compute_schema(Schemas, #socket_output{}) ->
    {error, {badarg, Schemas}}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: socket_output()) ->
    {ok, NewState :: socket_output()}
  | {error, {badarg, Term :: term()}}
  | {error, Reason :: term()}
.

%% @private
%% @doc Parses initializing arguments to set up the internal state of
%%      the input node.
init([], State = #socket_output{}) ->
    post_init(State)
;

init([{port_no, Port} | Args], State = #socket_output{}) ->
    init(Args, State#socket_output{port_no=Port})
;

init([{address, Addr} | Args], State = #socket_output{}) ->
    init(Args, State#socket_output{address=Addr})
;

init([Term | _Args], #socket_output{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

-spec post_init(State :: socket_output()) ->
    {ok, State :: socket_output()}
  | {error, Reason :: term()}
.

%% @private
%% @doc Opens a socket at the specified port number and listens for
%%      an incoming connection. (TODO)
post_init(State = #socket_output{port_no = PortNo, address = Addr}) ->
    case gen_tcp:connect(Addr, PortNo, []) of
        {ok, Sock} ->
            {ok, State#socket_output{socket=Sock}};
        {error, Reason} ->
            {error, Reason}
    end
.

%%% =============================================================== %%%
%%%  private tests                                                  %%%
%%% =============================================================== %%%

-ifdef(TEST).
init_test() ->
    ?assertMatch(
        {error, {badarg, bad}}
      , init([bad], #socket_output{})
    )
.
-endif.
