%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc This module reads input from a socket and pushes it to the
%%      query planner.
-module(ydb_socket_output).
-behaviour(ydb_plan_node).

-export([start_link/2]).
-export([init/1, delegate/2, delegate/3]).

-record(socket_input,
    {port_no :: integer(), socket :: port(), address}).

-type option() ::
    {port_no, integer()}
.

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
    {ok, State :: #socket_input{}}
  | {error, {badarg, Term :: term()}}
.

%% @doc Initializes the output node's internal state.
init(Args) when is_list(Args) -> init(Args, #socket_input{}).

%% ----------------------------------------------------------------- %%

-spec delegate(Request :: atom(), State :: #socket_input{}) ->
    {ok, State :: #socket_input{}}
.

%% @doc Sends received tuples out through a socket at the
%%      port and address initially specified.
delegate(
    _Request = {tuples, Tuples}
  , State = #socket_input{socket = Sock}
) when is_list(Tuples) ->
    ydb_socket_utils:send_tuples(Sock, Tuples)
  , {ok, State}
;

delegate(_Request = {tuple, Tuple}, State = #socket_input{}) ->
    delegate({tuples, [Tuple]}, State)
;

delegate(
    _Request = {info, Info = {tuples, Tuples}}
  , State = #socket_input{}
) when is_list(Tuples) ->
    delegate(Info, State)
;

delegate(
    _Request = {info, _Info = {tuple, Tuple}}
  , State = #socket_input{}
) ->
    delegate({tuples, [Tuple]}, State)
;

delegate(_Request, State) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec delegate(
    Request :: atom()
  , State :: #socket_input{}
  , Extras :: list()
) ->
    {ok, NewState :: #socket_input{}}
.

%% @doc Dummy implementation to satisfy plan_ndoe behavior.
delegate(_Request, State, _Extras) ->
    {ok, State}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: #socket_input{}) ->
    {ok, NewState :: #socket_input{}}
  | {error, {badarg, Term :: term()}}
  | {error, Reason :: term()}
.

%% @doc Parses initializing arguments to set up the internal state of
%%      the input node.
init([], State = #socket_input{}) ->
    post_init(State)
;

init([{port_no, Port} | Args], State = #socket_input{}) ->
    init(Args, State#socket_input{port_no=Port})
;

init([{address, Addr} | Args], State = #socket_input{}) ->
    init(Args, State#socket_input{address=Addr})
;

init([Term | _Args], #socket_input{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

-spec post_init(State :: #socket_input{}) ->
    {ok, State :: #socket_input{}}
  | {error, Reason :: term()}
.

%% @doc Opens a socket at the specified port number and listens for
%%      an incoming connection. (TODO)
post_init(State = #socket_input{port_no = PortNo, address = Addr}) ->
    case gen_tcp:connect(Addr, PortNo, []) of
        {ok, Sock} ->
            {ok, State#socket_input{socket=Sock}};
        {error, Reason} ->
            {error, Reason}
    end
.

%%% =============================================================== %%%
%%%  private tests                                                  %%%
%%% =============================================================== %%%

%% TODO.
