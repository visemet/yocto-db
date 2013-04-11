%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc This module reads input from a socket and pushes it to the
%%      query planner.
-module(ydb_socket_input).
-behaviour(ydb_plan_node).

-export([start_link/2]).
-export([init/1, delegate/2, delegate/3]).

% Testing for private functions.
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(socket_input, {port_no :: integer(), socket :: port()}).

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

%% @doc Initializes the input node's internal state.
init(Args) when is_list(Args) -> init(Args, #socket_input{}).

-spec delegate(Request :: atom(), State :: #socket_input{}) ->
    {ok, State :: #socket_input{}}
.

%% @doc Allows the input node to accept a connection.
delegate(_Request = {accept}, State = #socket_input{socket = LSock}) ->
    case gen_tcp:accept(LSock) of
        {ok, ASock} ->
            {ok, State#socket_input{socket=ASock}};

        {error, Reason} ->
            io:fwrite("Error: ~p, ~p~n", [Reason, self()]),
            {ok, State}
    end
;

%% @doc Allows the input node to accept data and convert it into a tuple
%%      to be pushed along the stream.
% TODO: handle socket closing.
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

delegate(_Request, State) ->
    {ok, State}
.

-spec delegate(
    Request :: atom()
  , State :: #socket_input{}
  , Extras :: list()
) ->
    {ok, NewState :: #socket_input{}}
.

%% @doc Allows the input node to accept data, and request the schema
%%      and timestamp so that it can be converted into a tuple.
delegate(
    _Request = {data, Data}
  , State = #socket_input{}
  , _Extras = [Schema, Timestamp]
) ->
    lists:foreach(fun(X) -> ydb_input_node_utils:push(
        ydb_input_node_utils:make_tuple(Timestamp, Schema, X)) end, Data)
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

%% @doc Opens a socket at the specified port number and listens for
%%      an incoming connection.
post_init(State = #socket_input{port_no = PortNo}) ->
    {ok, LSock} = gen_tcp:listen(PortNo, [{active, true}, binary])
  , ydb_plan_node:relegate(erlang:self(), {accept})
  , State#socket_input{socket=LSock}
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

