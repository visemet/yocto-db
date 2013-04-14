%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc This module reads input from a socket and pushes it to the
%%      query planner.
-module(ydb_socket_input).
-behaviour(ydb_plan_node).

-export([start_link/2, start_link/3]).
-export([init/1, delegate/2, delegate/3]).
-export([accept/2]).

% Testing for private functions.
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(socket_input, {
    port_no=9001 :: integer()
  , socket :: port()
  , acceptor :: pid()
}).

-type option() ::
    {port_no, PortNo :: integer()}.
%% Options for the socket input:
%% <ul>
%%   <li><code>{port_no, PortNo}</code> - Listens for input on the port
%%       specified by <code>PortNo</code>.</li>
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

%% ----------------------------------------------------------------- %%

-spec init(Args :: [option()]) ->
    {ok, State :: #socket_input{}}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Initializes the input node's internal state.
init(Args) when is_list(Args) -> init(Args, #socket_input{}).

%% ----------------------------------------------------------------- %%

-spec delegate(Request :: atom(), State :: #socket_input{}) ->
    {ok, State :: #socket_input{}}
.

%% @private
%% @doc Starts a linked process that will keep listening for and
%%      accepting new connections.
delegate(_Request = {accept}, State = #socket_input{socket = LSock}) ->
    process_flag(trap_exit, true)
  , APid = spawn_link(?MODULE, accept, [LSock, self()])
  , {ok, State#socket_input{acceptor = APid}}
;

% Allows the input node to accept data and convert it into tuples to be
% pushed along the stream.
delegate(
    _Request = {info, _Info = {tcp, _Socket, RawData}}
  , State = #socket_input{}
) ->
    ydb_plan_node:relegate(
        erlang:self()
      , {data, binary_to_term(RawData)}
      , [schema, timestamp]
      )

  , {ok, State}
;

% Restarts the acceptor process if it exits for any reason.
delegate(
    _Request = {info, _Info = {'EXIT', FromPid, _Reason}}
  , State = #socket_input{acceptor = APid}
) ->
    IsAcceptor = FromPid == APid
  , if IsAcceptor ->
        ydb_plan_node:relegate(erlang:self(), {accept})
      , {ok, State}
  ; true ->
      {ok, State}
    end
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

%% @private
%% @doc Allows the input node to accept data, and request the schema
%%      and timestamp so that it can be converted into a tuple.
delegate(
    _Request = {data, Data}
  , State = #socket_input{}
  , _Extras = [Schema, Timestamp]
) ->
    lists:foreach(fun(X) -> ydb_input_node_utils:push(
        ydb_input_node_utils:make_tuple(Timestamp, Schema, X)) end
      , Data
    )
  , {ok, State}
;

delegate(_Request, State, _Extras) ->
    {ok, State}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: #socket_input{}) ->
    {ok, NewState :: #socket_input{}}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Parses initializing arguments to set up the internal state of
%%      the input node.
init([], State = #socket_input{}) ->
    NewState = post_init(State)
  , {ok, NewState}
;

init([{port_no, Port} | Args], State = #socket_input{}) ->
    init(Args, State#socket_input{port_no=Port})
;

init([Term | _Args], #socket_input{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

-spec post_init(State :: #socket_input{}) -> State :: #socket_input{}.

%% @private
%% @doc Opens a socket at the specified port number and listens for
%%      an incoming connection.
post_init(State = #socket_input{port_no = PortNo}) ->
    {ok, LSock} = gen_tcp:listen(PortNo, [{active, true}, binary])
  , ydb_plan_node:relegate(erlang:self(), {accept})
  , State#socket_input{socket=LSock}
.

%% ----------------------------------------------------------------- %%

-spec accept(LSock :: port(), Pid :: pid()) -> no_return().

%% @private
%% @doc Listens for incoming connections using the passed in listener,
%%      and transfers ownership of the resulting socket to the
%%      specified Pid. Then recursively calls itself to listen again.
accept(LSock, Pid) ->
    case gen_tcp:accept(LSock) of
        {ok, ASock} ->
            gen_tcp:controlling_process(ASock, Pid),
            accept(LSock, Pid);
        {error, _Reason} ->
            accept(LSock, Pid)
    end
.

%%% =============================================================== %%%
%%%  private tests                                                  %%%
%%% =============================================================== %%%

-ifdef(TEST).
init_test() ->
    ?assertMatch(
        {ok, #socket_input{port_no=1337, socket=_Socket1}}
      , init([], #socket_input{port_no=1337})
    )
  , ?assertMatch(
        {ok, #socket_input{port_no=8000, socket=_Socket2}}
      , init([{port_no, 8000}], #socket_input{})
    )
  , ?assertMatch(
        {error, {badarg, bad}}
      , init([bad], #socket_input{})
    )
.
-endif.

%% ----------------------------------------------------------------- %%
