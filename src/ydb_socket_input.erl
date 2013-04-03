-module(ydb_socket_input).
-behaviour(ydb_plan_node).

-export([start_link/2]).
-export([init/1, delegate/2]).

-record(socket_input, {port_no}).

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

start_link(Args, Options) ->
    ydb_plan_node:start_link(?MODULE, Args, Options)
.

%% ----------------------------------------------------------------- %%

init(Args) when is_list(Args) -> init(Args, #socket_input{}).

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
