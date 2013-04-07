%% @author Kalpana Suraesh <ksuraesh@caltech.edu>

%% @doc This module reads input from a socket and pushes it to the
%%      query planner.
-module(ydb_socket_input).
-behaviour(ydb_plan_node).

-export([start_link/2]).
-export([init/1, delegate/2, delegate/3]).

-record(socket_input, {port_no :: integer(), socket :: port()}).


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
    {ok, State :: #socket_input{}}
  | {error, {badarg, Term :: term()}}
.

%% @doc TODO
init(Args) when is_list(Args) -> init(Args, #socket_input{}).

-spec delegate(Request :: atom(), State :: #socket_input{}) ->
    {ok, State :: #socket_input{}}
.

%% @doc TODO
delegate(_Request = {accept}, State = #socket_input{socket = LSock}) ->
    case gen_tcp:accept(LSock) of
        {ok, ASock} ->
            ?TRACE("accepted")
          , {ok, State#socket_input{socket=ASock}};

        {error, Reason} ->
            io:fwrite("Error: ~p, ~p~n", [Reason, self()]),
            {ok, State}
    end
;

%% @doc TODO
% TODO: handle socket closing.
delegate(
    _Request = {info, _Info = {tcp, _Socket, RawData}}
  , State = #socket_input{}
) ->
    ydb_plan_node:relegate(
        erlang:self(), {data, binary_to_term(RawData)}, [schema, timestamp])
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

%% @doc TODO
delegate(
    _Request = {data, Data}
  , State = #socket_input{}
  , _Extras = [Schema, Timestamp]
) ->
    ydb_input_node_utils:push(
        ydb_input_node_utils:make_tuple(Timestamp, Schema, Data))
  , {ok, State}
;

delegate(_Request, State, _Extras) ->
    {ok, State}
.


%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

-spec init(list(), State :: #socket_input{}) ->
    {ok, NewState :: #socket_input{}}
  | {error, {badarg, Term :: term()}}
.

%% @doc TODO
init([], State = #socket_input{}) ->
    NewState = post_init(State)
  , {ok, NewState}
;

%TODO: should probably be able to accept options and/or sockets as well.
init([{port_no, Port} | Args], State = #socket_input{}) ->
    init(Args, State#socket_input{port_no=Port})
;

init([Term | _Args], #socket_input{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

-spec post_init(State :: #socket_input{}) -> State :: #socket_input{}.

%% @doc TODO
post_init(State = #socket_input{port_no = PortNo}) ->
    {ok, LSock} = gen_tcp:listen(PortNo, [{active, true}, binary])
  , ydb_plan_node:relegate(erlang:self(), {accept})
  , State#socket_input{socket=LSock}.

%% ----------------------------------------------------------------- %%

