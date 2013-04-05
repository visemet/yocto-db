%% @author Kalpana Suraesh <TODO>

%% @doc Module containing functions used for reading tuples from a
%%      socket. Creates tuples from data received from the sockets.
-module(ydb_socket_input).
-behaviour(ydb_plan_node).

-export([start_link/2]).
-export([init/1, delegate/2]).

-record(socket_input, {port_no :: integer()}).

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
delegate(_Request, State) ->
    {ok, State}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

init([], State = #socket_input{}) ->
    post_init(State)

  , {ok, State}
;

init([Term | _Args], #socket_input{}) ->
    {error, {badarg, Term}}
.

%% ----------------------------------------------------------------- %%

post_init(#socket_input{}) ->
    pass
.

%% ----------------------------------------------------------------- %%
