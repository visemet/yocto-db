%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc This module reads input from a socket and pushes it to the
%%      query planner.
-module(ydb_branch_socket_input).
-behaviour(ydb_plan_node).

-export([start_link/2, start_link/3]).
-export([init/1, delegate/2, delegate/3, compute_schema/2]).
-export([accept/2]).

% Testing for private functions.
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(socket_input, {
    port_no=9002 :: integer()
  , socket :: port()
  , acceptor :: pid()
}).

-type socket_input() :: #socket_input{
    port_no :: integer()
  , socket :: undefined | port()
  , acceptor :: undefined | pid()}.
%% Internal socket input node state.

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
    {ok, State :: socket_input()}
  | {error, {badarg, Term :: term()}}
.

%% @private
%% @doc Initializes the input node's internal state.
init(Args) when is_list(Args) -> init(Args, #socket_input{}).

%% ----------------------------------------------------------------- %%

-spec delegate(Request :: atom(), State :: socket_input()) ->
    {ok, State :: socket_input()}
.

%% @private
%% @doc Starts a linked process that will keep listening for and
%%      accepting new connections.
delegate(_Request = {accept}, State = #socket_input{socket = LSock}) ->
    process_flag(trap_exit, true)
  , APid = spawn_link(?MODULE, accept, [LSock, self()])
  , {ok, State#socket_input{acceptor = APid}}
;

%%      Allows the input node to accept data and convert it into tuples
%%      to be pushed along the stream.
delegate(
    _Request = {info, _Info = {tcp, _Socket, RawData}}
  , State = #socket_input{}
) ->
    ydb_plan_node:notify(
        erlang:self()
      , {tuples, erlang:binary_to_term(RawData)}
    )

  , {ok, State}
;

%%      Restarts the acceptor process if it exits for any reason.
delegate(
    _Request = {info, _Info = {'EXIT', FromPid, _Reason}}
  , State = #socket_input{acceptor = APid}
) ->
    if
        FromPid == APid -> ydb_plan_node:relegate(erlang:self(), {accept})
      
      ; true -> pass
    end

  , {ok, State}
;

delegate(_Request, State) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec delegate(
    Request :: atom()
  , State :: socket_input()
  , Extras :: list()
) ->
    {ok, NewState :: socket_input()}
.

%% @doc Does nothing.
delegate(_Request, State, _Extras) ->
    {ok, State}
.

%% ----------------------------------------------------------------- %%

-spec compute_schema(
    InputSchemas :: [ydb_plan_node:ydb_schema()]
  , State :: socket_input()
) ->
    {error, {badarg, InputSchemas :: [ydb_plan_node:ydb_schema()]}}
.

%% @doc Returns the output schema of the socket input node based upon
%%      the supplied input schemas. Not supported, as the schema should
%%      only be defined during initialization.
compute_schema(Schemas, #socket_input{}) ->
    {error, {badarg, Schemas}}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: socket_input()) ->
    {ok, NewState :: socket_input()}
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

-spec post_init(State :: socket_input()) -> State :: socket_input().

%% @private
%% @doc Opens a socket at the specified port number and listens for
%%      an incoming connection.
post_init(State = #socket_input{port_no = PortNo}) ->
    {ok, LSock} = gen_tcp:listen(
        PortNo, [{active, true}, binary, {reuseaddr, true}])
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
        {ok, #socket_input{port_no=1437, socket=_Socket1}}
      , init([], #socket_input{port_no=1437})
    )
  , ?assertMatch(
        {ok, #socket_input{port_no=8100, socket=_Socket2}}
      , init([{port_no, 8100}], #socket_input{})
    )
  , ?assertMatch(
        {error, {badarg, bad}}
      , init([bad], #socket_input{})
    )
.
-endif.

%% ----------------------------------------------------------------- %%
