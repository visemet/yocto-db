%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc This module receives tuples from the query planner and outputs
%%      it to a socket.
-module(ydb_socket_output).
-behaviour(ydb_plan_node).

-export([start_link/2]).
-export([init/1, delegate/2, delegate/3]).

% Testing for private functions.
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(socket_output, {
    port_no=9001 :: integer()
  , socket :: port()
  , address :: term()
}).

-type option() ::
    {port_no, integer()}
  | {address, term()}
.

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
    {ok, State :: #socket_output{}}
  | {error, {badarg, Term :: term()}}
.

%% @doc Initializes the output node's internal state.
init(Args) when is_list(Args) -> init(Args, #socket_output{}).

%% ----------------------------------------------------------------- %%

-spec delegate(Request :: atom(), State :: #socket_output{}) ->
    {ok, State :: #socket_output{}}
.

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
  , State :: #socket_output{}
  , Extras :: list()
) ->
    {ok, NewState :: #socket_output{}}
.

%% @doc Dummy implementation to satisfy plan_node behavior.
delegate(_Request, State, _Extras) ->
    {ok, State}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init([option()], State :: #socket_output{}) ->
    {ok, NewState :: #socket_output{}}
  | {error, {badarg, Term :: term()}}
  | {error, Reason :: term()}
.

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

-spec post_init(State :: #socket_output{}) ->
    {ok, State :: #socket_output{}}
  | {error, Reason :: term()}
.

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

-ifdef(TEST). % TODO
init_test() ->
%    ?assertMatch(
%        {ok, #socket_output{port_no=1337}}
%      , init([], #socket_output{port_no=1337})
%    )
  %, ?assertMatch(
  %      {ok, #socket_output{port_no=9000, address={127,0,0,1}}}
  %    , init([{port_no, 9000}, {address, {127,0,0,1}}], #socket_output{})
  %  )
  ?assertMatch(
        {error, {badarg, bad}}
      , init([bad], #socket_output{})
    )
.
-endif.
