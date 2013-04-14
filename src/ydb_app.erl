-module(ydb_app).
-behaviour(application).

-export([register_input_stream/1, register_query/1]).
-export([start/2, stop/1]).

%%% =============================================================== %%%
%%%  API                                                            %%%
%%% =============================================================== %%%

register_input_stream(Spec) ->
    ydb_sup:register_input_stream(Spec)
.

register_query(Spec) ->
    ydb_sup:register_query(Spec)
.

%% ----------------------------------------------------------------- %%

start(normal, _Args) ->
    ydb_sup:start_link()
.

stop(_State) -> ok.

%%% =============================================================== %%%
%%%  private functions                                              %%%
%%% =============================================================== %%%



%% ----------------------------------------------------------------- %%
