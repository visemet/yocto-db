-module(plan_node).
-behaviour(gen_server).

-export([start/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(plan_node, {type, schema, listeners=[], wrapped}).

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

-callback init(Options :: term()) ->
    {ok, State :: term()}
  | {error, Reason :: term()}.

%% ----------------------------------------------------------------- %%

-type schema() :: [{atom(), {pos_integer(), atom()}}].

%% ----------------------------------------------------------------- %%

start(Name, Type, Schema, Options) ->
    gen_server:start({local, Name}, ?MODULE, Options, [])
.

subscribe(Listener) when is_pid(Listener) -> pass.

unsubscribe(Listener) when is_pid(Listener) -> pass.

%% ----------------------------------------------------------------- %%

init([Type, Schema, Options]) ->
    State = #plan_node{
        type=Type
      , schema=Schema
      , wrapped=Type:init(Options)
    }

  , {ok, State}
.

handle_call(_Request, _From, State) -> {reply, ok, State}.

handle_cast(_Request, State) -> {noreply, State}.

handle_info(_Info, State) -> {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% ----------------------------------------------------------------- %%
