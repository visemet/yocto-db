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
delegate(Request = {listen}, State = #socket_input{}) ->
    % @max, you had something like ydb_plan_node:notify() in ydb_file_input
    % here, but I can't figure out what it does...?
    gen_server:cast(erlang:self(), {delegate, Request, [schema, timestamp]})
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
    Request = {listen}
  , State = #socket_input{socket=Socket}
  , _Extras = [Schema, Timestamp]
) ->
    inet:setopts(Socket, [{active, once}])
  , receive
        {tcp, Socket, BinData} ->
            ydb_input_node_utils:push(
                ydb_input_node_utils:make_tuple(Timestamp, Schema,
                    binary_to_term(BinData))
            )
    end
  , gen_server:cast(erlang:self(), {delegate, Request})
  , {ok, State};

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
    {ok, LSock} = gen_tcp:listen(PortNo, [{active, false}, binary])
  , {ok, ASock} = gen_tcp:accept(LSock)
  , gen_server:cast(erlang:self(), {delegate, {listen}})
  , State#socket_input{socket=ASock}.

%% ----------------------------------------------------------------- %%
