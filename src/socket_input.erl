-module(socket_input).
-behaviour(plan_node).

-export([start/3]).
-export([init/1, delegate/2]).

-record(socket_input, {port_no}).

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

start(Name, Schema, Options) ->
    plan_node:start(Name, ?MODULE, Schema, Options)
.

%% ----------------------------------------------------------------- %%

init(Options) when is_list(Options) -> init(Options, #socket_input{}).

init([], State = #socket_input{}) ->
    post_init(State)

  , {ok, State}
;

init([Term | _Options], #socket_input{}) ->
    {error, {badarg, Term}}
.

delegate(_Request, State) ->
    {ok, State}
.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%

post_init(#socket_input{}) ->
    pass
.

%% ----------------------------------------------------------------- %%
